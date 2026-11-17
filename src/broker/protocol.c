#define _POSIX_C_SOURCE 200809L

#include "protocol.h"
#include "net_util.h"  
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#define MAX_WORDS 8
#define SEPARATOR " "


static int parse_long (const char *text, long *result){
    char *end = NULL;

    errno = 0;

    long value = strtol (text, &end, 10);

    if(errno != 0 || end == text || *end !='\0')
        return -1;

    *result = value;
    return 0;
}


static int fail(command_t *command, const char *reason){
    command->type = C_INVALID;

    snprintf(command->error, sizeof command->error, "%s", reason);

    
    return -1;
}


int protocol_parse(const char *line, command_t *command){
    memset(command, 0, sizeof *command);

    char copy[LINE_MAX_LEN];
    snprintf(copy, sizeof copy, "%s", line);

    char *words[MAX_WORDS];
    int count = 0;
    char *save = NULL;

    for(char *t = strtok_r(copy, SEPARATOR, &save); t != NULL; t = strtok_r(NULL, SEPARATOR, &save)){
        if(count == MAX_WORDS)
            return fail(command, "too many arguments");
        
        words[count++] = t;
    }

    if(count == 0)
        return fail(command, "empty command");


    //SHUTDOWN
    if(strcmp(words[0], "SHUTDOWN") == 0){
        if(count != 1)
            return fail(command, "command takes no arguments");

        command->type = C_SHUTDOWN;
        return 0;
    }
    

    //BROKER PAUSE | RESUME
    if(strcmp(words[0], "BROKER") == 0){
        long broker_id;

        if(count != 3)
            return fail(command, "expected BROKER PAUSE|RESUME <id>");

        if(strcmp(words[1], "PAUSE") != 0 && strcmp(words[1], "RESUME") != 0)
            return fail(command, "expected PAUSE or RESUME");

        if(parse_long(words[2], &broker_id) < 0 || broker_id < 0)
            return fail(command, "invalid broker id");

        command->broker_id = (int)broker_id;

        command->type = (strcmp(words[1], "PAUSE") == 0) ? C_BROKER_PAUSE : C_BROKER_RESUME;

        return 0;
    }

    
    //PRODUCE
    if(strcmp(words[0], "PRODUCE") == 0){
        long acks;
        long key_len;
        long value_len;

        if(count != 5)
            return fail(command, "expected PRODUCE <topic> <acks> <key_len> <value_len>");

        if(strlen(words[1]) >= TOPIC_NAME_MAX)
            return fail(command, "topic name too long");

        if(strcmp(words[2], "ALL") == 0)
            acks = -1;


        else if(parse_long(words[2], &acks) < 0 || (acks != 0 && acks != 1))
            return fail(command, "invalid acks");


        if(parse_long(words[3], &key_len) < 0 || key_len < 0 || key_len > MAX_KEY_BYTES)    
            return fail(command, "invalid key size");


        if(parse_long(words[4], &value_len) < 0 || value_len <= 0 || value_len > MAX_VALUE_BYTES)
            return fail(command, "invalid value size");


        snprintf(command->topic, sizeof command->topic, "%s", words[1]);
        command->acks = (int)acks;
        command->key_len = (size_t)key_len;
        command->value_len = (size_t)value_len;

        command->type = C_PRODUCE;

        return 0;

    }


    //JOIN
    if(strcmp(words[0], "JOIN") == 0){

        if(count < 3 || count > 4)
            return fail(command, "expected JOIN <topic> <group> [FROM_BEGINNING]");
        
        if(strlen(words[1]) >= TOPIC_NAME_MAX)
            return fail(command, "topic name too long");

        if(strlen(words[2]) >= GROUP_NAME_MAX)
            return fail(command, "group name too long");

        snprintf(command->topic, sizeof command->topic, "%s", words[1]);
        snprintf(command->group, sizeof command->group, "%s", words[2]);

        if(count == 4){
            if(strcmp(words[3], "FROM_BEGINNING") != 0)
                return fail(command, "expected FROM_BEGINNING");

            command->from_beginning = 1;
        }

        command->type = C_JOIN;
        return 0;
        
    }


    //POLL
    if(strcmp(words[0], "POLL") == 0){
        long max_bytes;
        long wait_ms;

        if(count != 5)
            return fail(command, "expected POLL <topic> <group> <max_bytes> <wait_ms>");

        if(strlen(words[1]) >= TOPIC_NAME_MAX)
            return fail(command, "topic name too long");


        if(strlen(words[2]) >= GROUP_NAME_MAX)
            return fail(command, "group name too long");


        if(parse_long(words[3], &max_bytes) < 0 || max_bytes <= 0 || max_bytes > MAX_RESPONSE_BYTES)
            return fail(command, "invalid max bytes");


        if(parse_long(words[4], &wait_ms) < 0 || wait_ms < 0 || wait_ms > INT_MAX)
            return fail(command, "invalid wait time");


        snprintf(command->topic, sizeof command->topic, "%s", words[1]);
        snprintf(command->group, sizeof command->group, "%s", words[2]);
        command->max_bytes = (size_t)max_bytes;
        command->wait_ms = (int)wait_ms;

        command->type = C_POLL;
        return 0;
    }
    
    
    //COMMIT
    if(strcmp(words[0], "COMMIT") == 0){
        long generation;
        long partition;
        long offset;

        if(count != 6)
            return fail(command, "expected COMMIT <topic> <group> <generation> <partition> <offset>");


        if(strlen(words[1]) >= TOPIC_NAME_MAX)
            return fail(command, "topic name too long");

            
        if(strlen(words[2]) >= GROUP_NAME_MAX)
            return fail(command, "group name too long");


        if(parse_long(words[3], &generation) < 0 || generation < 0)
            return fail(command, "invalid generation");


        if(parse_long(words[4], &partition) < 0 || partition < 0 || partition > INT_MAX)
            return fail(command, "invalid partition");


        if(parse_long(words[5], &offset) < 0 || offset < 0)
            return fail(command, "invalid offset");


        snprintf(command->topic, sizeof command->topic, "%s", words[1]);
        snprintf(command->group, sizeof command->group, "%s", words[2]);
        command->generation = generation;
        command->partition = (int)partition;
        command->offset = offset;

        command->type = C_COMMIT;
        return 0;
    }


    //LEAVE
    if(strcmp(words[0], "LEAVE") == 0){

        if(count != 3)
            return fail(command, "expected LEAVE <topic> <group>");


        if(strlen(words[1]) >= TOPIC_NAME_MAX)
            return fail(command, "topic name too long");


        if(strlen(words[2]) >= GROUP_NAME_MAX)
            return fail(command, "group name too long");


        snprintf(command->topic, sizeof command->topic, "%s", words[1]);
        snprintf(command->group, sizeof command->group, "%s", words[2]);

        command->type = C_LEAVE;
        return 0;
    }



    //GROUP DESCRIBE
    if(strcmp(words[0], "GROUP") == 0){

        if(count < 2)
            return fail(command, "expected GROUP DESCRIBE <group>");


        if(strcmp(words[1], "DESCRIBE") == 0){

            if(count != 3)
                return fail(command, "expected GROUP DESCRIBE <group>");


            if(strlen(words[2]) >= GROUP_NAME_MAX)
                return fail(command, "group name too long");

            snprintf(command->group, sizeof command->group, "%s", words[2]);

            command->type = C_GROUP_DESCRIBE;
            return 0;
        }

        return fail(command, "unknown GROUP subcommand");
    }


    //TOPIC
    if(strcmp(words[0], "TOPIC") == 0){

        if(count < 2)
            return fail(command, "expected TOPIC CREATE, LIST or DESCRIBE");

        //CREATE
        if(strcmp(words[1], "CREATE") == 0){
            long partitions;
            long replication;
            long retention = 0; 

            if(count < 5 || count > 6)
                return fail(command, "expected TOPIC CREATE <topic> <partitions> <replication> [retention_bytes]");


            if(strlen(words[2]) >= TOPIC_NAME_MAX)
                return fail(command, "topic name too long");


            if(parse_long(words[3], &partitions) < 0 || partitions <= 0)
                return fail(command, "invalid partitions");


            if(parse_long(words[4], &replication) < 0 || replication <= 0)
                return fail(command, "invalid replication");


            if(count == 6 && (parse_long(words[5], &retention) < 0 || retention < 0))
                return fail(command, "invalid retention bytes");


            snprintf(command->topic, sizeof command->topic, "%s", words[2]);
            command->partitions = (int)partitions;
            command->replication = (int)replication;
            command->retention_bytes = retention;

            command->type = C_TOPIC_CREATE;
            return 0;
        }

        //LIST
        if(strcmp(words[1], "LIST") == 0){

            if(count != 2)
                return fail(command, "TOPIC LIST takes no arguments");

            command->type = C_TOPIC_LIST;
            return 0;
        }

        //DESCRIBE
        if(strcmp(words[1], "DESCRIBE") == 0){

            if(count != 3)
                return fail(command, "expected TOPIC DESCRIBE <topic>");

            if(strlen(words[2]) >= TOPIC_NAME_MAX)
                return fail(command, "topic name too long");

            snprintf(command->topic, sizeof command->topic, "%s", words[2]);

            command->type = C_TOPIC_DESCRIBE;
            return 0;
        }


        return fail(command, "unknown TOPIC subcommand");
    }

    return fail(command, "unknown command");

}



int protocol_send_response(int file_descriptor, int ok, const char *body){
    size_t len = (body != NULL) ? strlen(body) : 0;

    char header[64];

    snprintf(header, sizeof header, "%s %zu", ok ? "OK" : "ERR", len);

    if(nu_send_line(file_descriptor, header) <0)
        return -1;

    if(len == 0)
        return 0;

    return nu_write_all(file_descriptor, body, len);
}



int protocol_send_response_bytes(int file_descriptor, int ok, const char *body, size_t len){

    if(len > MAX_RESPONSE_BYTES)
        return -1;

    char header[64];

    snprintf(header, sizeof header, "%s %zu", ok ? "OK" : "ERR", len);

    if(nu_send_line(file_descriptor, header) <0)
        return -1;

    if(len == 0)
        return 0;
    
    return nu_write_all(file_descriptor, body, len);
}



int protocol_read_response(int file_descriptor, int *ok, char **body, size_t *len){
    
    char line[LINE_MAX_LEN];

    if(nu_read_line(file_descriptor, line, sizeof line) < 0)
        return -1;

    char *space = strchr(line, ' ');

    if(space == NULL)
        return -1;

    *space = '\0';

    if(strcmp(line, "OK") == 0)
        *ok = 1;

    else if(strcmp(line, "ERR") == 0)
        *ok = 0;

    else 
        return -1;

    long nbytes;

    if(parse_long(space + 1, &nbytes) < 0 || nbytes < 0 || nbytes > MAX_RESPONSE_BYTES)
        return -1;

    char *buffer = malloc((size_t)nbytes + 1);

    if(buffer == NULL)
        return -1;

    if (nbytes > 0 && nu_read_exact(file_descriptor, buffer, (size_t)nbytes) < 0){
        free(buffer);
        return -1;
    }

    buffer[nbytes] = '\0';

    *body = buffer;
    *len = (size_t)nbytes;
    return 0;
}



int protocol_send_produce(int file_descriptor, const char *topic, int acks, const char *key, size_t key_len, const char *value, size_t value_len){
    
    if(strlen(topic) >= TOPIC_NAME_MAX || key_len > MAX_KEY_BYTES || value_len == 0 || value_len > MAX_VALUE_BYTES)
        return -1;

    char line[LINE_MAX_LEN];

    if(acks == -1)
        snprintf(line, sizeof line, "PRODUCE %s ALL %zu %zu", topic, key_len, value_len);

    else
        snprintf(line, sizeof line, "PRODUCE %s %d %zu %zu", topic, acks, key_len, value_len);

    if(nu_send_line(file_descriptor, line) < 0)
        return -1;

    if(key_len > 0 && nu_write_all(file_descriptor, key, key_len) < 0)
        return -1;
    
    return nu_write_all(file_descriptor, value, value_len);
}



int protocol_print_response(int file_descriptor, size_t *len){
    int ok;
    char *body;

    if(protocol_read_response(file_descriptor, &ok, &body, len) < 0)
        return -1;

    FILE *out = stdout;

    if(!ok)
        out = stderr;

    fwrite(body, 1, *len, out);

    if(*len > 0 && body[*len -1] != '\n')
        fputc('\n', out);

    fflush(out);

    free(body);
    return ok;
    
}
