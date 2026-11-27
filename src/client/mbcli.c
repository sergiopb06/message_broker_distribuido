#define _POSIX_C_SOURCE 200809L

#include "net_util.h"
#include "client.h"
#include "protocol.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define POLL_MAX_BYTES (1024 * 1024)
#define POLL_WAIT_MS 1000
#define MAX_ARGS 16

//Help text for wrong arguments. 
static void usage(const char *program){
    fprintf(stderr,
            "usage: %s <host:port | host port> <command>\n"
            "  topic create <topic> -p <partitions> -r <replication> [--retention-bytes <bytes>]\n"
            "  topic list\n"
            "  topic describe <topic>\n"
            "  produce <topic> [-k <key>] [--acks 0|1|all]   (one message per line of stdin; acks defaults to all)\n"
            "  consume <topic> -g <group> [--from-beginning]\n"
            "  group describe <group>\n"
            "  broker pause|resume <id>\n"
            "  shutdown\n"
            "The positional form of the protocol is also accepted:\n"
            "  TOPIC CREATE <topic> <partitions> <replication> [retention_bytes]\n"
            "  PRODUCE <topic> <0|1|ALL> [key]   CONSUME <topic> <group> [FROM_BEGINNING]\n"
            "  TOPIC LIST | TOPIC DESCRIBE <topic> | GROUP DESCRIBE <group> | BROKER PAUSE|RESUME <id> | SHUTDOWN\n",
            program);
}


static void upper(char *word){
    for(; *word != '\0'; word++)
        *word = (char)toupper((unsigned char)*word);
}


//Translates the flag form of a command (argv from index 3 on) into the positional
//form the rest of mbcli and the protocol use. Commands already written in
//uppercase pass through. Returns the new argc, or -1 on a usage error.
static int normalize_command(int argc, char **argv, char **out, char *storage, size_t storage_size){
    int n = 0;
    size_t used = 0;
    for(int i = 0; i < 3; i++)
        out[n++] = argv[i];

    if(argc < 4 || !islower((unsigned char)argv[3][0])){
        if(argc > MAX_ARGS)
            return -1;
        for(int i = 3; i < argc; i++)
            out[n++] = argv[i];
        return n;
    }

    //Copies a word into storage, uppercased if asked
    #define PUSH(word, up) do {                                         \
        size_t len_ = strlen(word) + 1;                                 \
        if(n >= MAX_ARGS || used + len_ > storage_size) return -1;      \
        memcpy(storage + used, (word), len_);                           \
        if(up) upper(storage + used);                                   \
        out[n++] = storage + used;                                      \
        used += len_;                                                   \
    } while(0)

    const char *command = argv[3];
    const char *topic = (argc > 4) ? argv[4] : NULL;

    if(strcmp(command, "topic") == 0 && argc >= 5 && strcmp(argv[4], "create") == 0){
        const char *partitions = NULL, *replication = NULL, *retention = NULL;
        if(argc < 6)
            return -1;
        for(int i = 6; i < argc; i += 2){
            if(i + 1 >= argc)
                return -1;
            if(strcmp(argv[i], "-p") == 0)
                partitions = argv[i + 1];
            else if(strcmp(argv[i], "-r") == 0)
                replication = argv[i + 1];
            else if(strcmp(argv[i], "--retention-bytes") == 0)
                retention = argv[i + 1];
            else
                return -1;
        }
        if(partitions == NULL || replication == NULL)
            return -1;
        PUSH("TOPIC", 0); PUSH("CREATE", 0); PUSH(argv[5], 0);
        PUSH(partitions, 0); PUSH(replication, 0);
        if(retention != NULL)
            PUSH(retention, 0);
        return n;
    }

    if(strcmp(command, "produce") == 0 && topic != NULL){
        const char *key = NULL, *acks = "ALL";
        for(int i = 5; i < argc; i += 2){
            if(i + 1 >= argc)
                return -1;
            if(strcmp(argv[i], "-k") == 0)
                key = argv[i + 1];
            else if(strcmp(argv[i], "--acks") == 0)
                acks = argv[i + 1];
            else
                return -1;
        }
        PUSH("PRODUCE", 0); PUSH(topic, 0); PUSH(acks, 1);
        if(key != NULL)
            PUSH(key, 0);
        return n;
    }

    if(strcmp(command, "consume") == 0 && topic != NULL){
        const char *group = NULL;
        int from_beginning = 0;
        for(int i = 5; i < argc; i++){
            if(strcmp(argv[i], "-g") == 0 && i + 1 < argc)
                group = argv[++i];
            else if(strcmp(argv[i], "--from-beginning") == 0)
                from_beginning = 1;
            else
                return -1;
        }
        if(group == NULL)
            return -1;
        PUSH("CONSUME", 0); PUSH(topic, 0); PUSH(group, 0);
        if(from_beginning)
            PUSH("FROM_BEGINNING", 0);
        return n;
    }

    //topic list|describe, group describe, broker pause|resume, shutdown: the
    //keywords are uppercased and the names (topic, group, id) kept as written
    if(argc > MAX_ARGS)
        return -1;
    for(int i = 3; i < argc; i++)
        PUSH(argv[i], i <= 4);
    return n;

    #undef PUSH
}




//PRODUCE <topic> <0|1|ALL> [key]
static int run_produce(const char *host, unsigned short port, int argc, char **argv){

    if(argc != 6 && argc != 7){
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    acks_t acks;

    if(strcmp(argv[5], "0") == 0)
        acks = ACKS_NONE;

    else if(strcmp(argv[5], "1") == 0)
        acks = ACKS_LEADER;

    else if(strcmp(argv[5], "ALL") == 0)
        acks = ACKS_ALL;

    else{
        usage(argv[0]);
        return EXIT_FAILURE;
    }


    const char *key = NULL;
    size_t key_len = 0;

    if(argc == 7){
        key = argv[6];
        key_len = strlen(key);
    }


    mb_client_t *client = mbc_connect(host, port);

    if(client == NULL)
        return EXIT_FAILURE;

    char *line = NULL;
    size_t capacity = 0;
    ssize_t read;
    int result = EXIT_SUCCESS;


    while((read = getline(&line, &capacity, stdin)) != -1){

        if(read > 0 && line[read - 1] == '\n')
            read--;   

        if(read == 0)
            continue;   


        int partition;
        long offset;
        int status = mbc_produce(client, argv[4], key, key_len, line, (size_t)read, acks, &partition, &offset);

        if(status != MB_OK){
            fprintf(stderr, "produce failed (error %d)\n", status);
            result = EXIT_FAILURE;
            break;
        }


        //Confirmed message
        if(offset >= 0)
            printf("partition=%d offset=%ld\n", partition, offset);
    }

    free(line);
    mbc_close(client);
    return result;
}



//CONSUME <topic> <group> [FROM_BEGINNING]
static int run_consume(const char *host, unsigned short port, int argc, char **argv){

    int from_beginning = 0;

    if(argc == 7 && strcmp(argv[6], "FROM_BEGINNING") == 0)
        from_beginning = 1;

    else if(argc != 6){
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    mb_client_t *client = mbc_connect(host, port);

    if(client == NULL)
        return EXIT_FAILURE;

    int status = mbc_join(client, argv[4], argv[5], from_beginning);


    while(status == MB_OK){
        message_t *msgs;
        size_t n;
        int partition;

        status = mbc_poll(client, POLL_MAX_BYTES, POLL_WAIT_MS, &msgs, &n, &partition);

        if(status == MB_OK){
            for(size_t i = 0; i < n; i++){
                printf("partition=%d offset=%ld ", partition, (long)msgs[i].offset);
                fwrite(msgs[i].value, 1, msgs[i].value_len, stdout);
                putchar('\n');
            }

            fflush(stdout);

            long next = 0;

            if(n > 0)
                next = (long)msgs[n - 1].offset + 1;

            free(msgs);


            if(n > 0)
                status = mbc_commit(client, partition, next);
        }


        if(status == MB_ERR_REBALANCE)
            status = mbc_join(client, argv[4], argv[5], from_beginning);
    }



    if(status != MB_OK)
        fprintf(stderr, "consume failed (error %d)\n", status);

    mbc_close(client);

    if(status != MB_OK)
        return EXIT_FAILURE;

    return EXIT_SUCCESS;
}





int main(int argc, char **argv) 
{
    //host:port as a single argument: split it into host and port
    char *split_argv[MAX_ARGS + 1];
    char host[256];
    if(argc >= 3 && strchr(argv[1], ':') != NULL && argc <= MAX_ARGS){
        const char *colon = strrchr(argv[1], ':');
        size_t host_len = (size_t)(colon - argv[1]);
        if(host_len == 0 || host_len >= sizeof host){
            fprintf(stderr, "invalid address '%s'\n", argv[1]);
            return EXIT_FAILURE;
        }
        memcpy(host, argv[1], host_len);
        host[host_len] = '\0';
        split_argv[0] = argv[0];
        split_argv[1] = host;
        split_argv[2] = (char *)(colon + 1);
        for(int i = 2; i < argc; i++)
            split_argv[i + 1] = argv[i];
        argc++;
        split_argv[argc] = NULL;
        argv = split_argv;
    }

    if(argc < 4){
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    char *normal_argv[MAX_ARGS + 1];
    char storage[LINE_MAX_LEN];
    int normal_argc = normalize_command(argc, argv, normal_argv, storage, sizeof storage);
    if(normal_argc < 0){
        usage(argv[0]);
        return EXIT_FAILURE;
    }
    normal_argv[normal_argc] = NULL;
    argc = normal_argc;
    argv = normal_argv;


    char *tail = NULL;
    errno = 0;
    long port = strtol(argv[2], &tail, 10);

    if(errno != 0 || tail == argv[2] || *tail != '\0' || port <= 0 || port > 65535){
        fprintf(stderr, "invalid port '%s'\n", argv[2]);
        return EXIT_FAILURE;
    }


    int produce = strcmp(argv[3], "PRODUCE") == 0;

    int consume = strcmp(argv[3], "CONSUME") == 0;


    if(produce)
        return run_produce(argv[1], (unsigned short)port, argc, argv);


    if(consume)
        return run_consume(argv[1], (unsigned short)port, argc, argv);


    char request[LINE_MAX_LEN];
    size_t used = 0;
    request[0] = '\0';

    for (int i = 3; i < argc; ++i){


        int w = snprintf(request + used, sizeof request - used, "%s%s", (i > 3) ? " " : "", argv[i]);

        if (w < 0 || (size_t)w >= sizeof request - used){
            fprintf(stderr, "command too long\n");
            return EXIT_FAILURE;
        }


        used += (size_t)w;
    }


    int file_descriptor = nu_connect(argv[1], (unsigned short)port);

    if (file_descriptor < 0)
        return EXIT_FAILURE;

    if(nu_send_line(file_descriptor, request) < 0){
        close(file_descriptor);
        return EXIT_FAILURE;
    }


    size_t len;
    int result = protocol_print_response(file_descriptor, &len);

    close(file_descriptor);

    if(result < 0){
        fprintf(stderr, "no response from server\n");
        return EXIT_FAILURE;
    }


    if(result == 0)
        return EXIT_FAILURE;



    return EXIT_SUCCESS;


}