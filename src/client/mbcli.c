#define _POSIX_C_SOURCE 200809L

#include "net_util.h"
#include "client.h"
#include "protocol.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define POLL_MAX_BYTES (1024 * 1024)
#define POLL_WAIT_MS 1000

//Help text for wrong arguments. 
static void usage(const char *program){
    fprintf(stderr,
             "usage: %s <host> <port> <command>\n"
            "  TOPIC CREATE <topic> <partitions> <replication> [retention_bytes]\n"
            "  TOPIC LIST\n"
            "  TOPIC DESCRIBE <topic>\n"
            "  PRODUCE <topic> <0|1|ALL> [key]   (one message per line of stdin)\n"
            "  CONSUME <topic> <group> [FROM_BEGINNING]\n"
            "  GROUP DESCRIBE <group>\n"
            "  BROKER PAUSE|RESUME <id>\n"
            "  SHUTDOWN\n", program);
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
    if(argc < 4){
        usage(argv[0]);
        return EXIT_FAILURE;
    }


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