#define _POSIX_C_SOURCE 200809L

#include "client.h"
#include "net_util.h"
#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


struct mb_client{
    int file_descriptor;
    char topic[TOPIC_NAME_MAX];   
    char group[GROUP_NAME_MAX];
    long generation;              
};



static int starts_with_word(const char *text, const char *word){
    size_t len = strlen(word);

    if(strncmp(text, word, len) != 0)
        return 0;

    return text[len] == '\0' || text[len] == ' ' || text[len] == '\n';
}


//the name the broker sends back to its mb_status_t
static int status_from_text(const char *text){
    if(starts_with_word(text, "UNKNOWN_TOPIC"))
        return MB_ERR_UNKNOWN_TOPIC;

    if(starts_with_word(text, "TOPIC_EXISTS"))
        return MB_ERR_TOPIC_EXISTS;

    if(starts_with_word(text, "NOT_LEADER"))
        return MB_ERR_NOT_LEADER;

    if(starts_with_word(text, "OFFSET_OUT_OF_RANGE"))
        return MB_ERR_OFFSET_OUT_OF_RANGE;

    if(starts_with_word(text, "REBALANCE"))
        return MB_ERR_REBALANCE;

    if(starts_with_word(text, "TIMEOUT"))
        return MB_ERR_TIMEOUT;

    if(starts_with_word(text, "SHUTTING_DOWN"))
        return MB_ERR_SHUTTING_DOWN;

    if(starts_with_word(text, "INVALID_ARGUMENT"))
        return MB_ERR_INVALID_ARGUMENT;

    return -1;   
}


static int read_reply(mb_client_t *client, char **body, size_t *len){
    int ok;

    *body = NULL;
    *len = 0;

    if(protocol_read_response(client->file_descriptor, &ok, body, len) < 0)
        return -1;


    if(ok)
        return MB_OK;


    return status_from_text(*body);
}


mb_client_t *mbc_connect(const char *host, unsigned short port){
    mb_client_t *client = calloc(1, sizeof *client);

    if(client == NULL)
        return NULL;


    client->file_descriptor = nu_connect(host, port);


    if(client->file_descriptor < 0){
        free(client);
        return NULL;
    }


    client->generation = -1;
    return client;
}


void mbc_close(mb_client_t *client){

    if(client == NULL)
        return;


    close(client->file_descriptor);
    free(client);
}


int mbc_request(mb_client_t *client, const char *line, char **body, size_t *len){

    *body = NULL;
    *len = 0;

    if(nu_send_line(client->file_descriptor, line) < 0)
        return -1;


    return read_reply(client, body, len);
}



//Broker replies "<partition> <offset>"
int mbc_produce(mb_client_t *client, const char *topic, const char *key, size_t key_len, const char *value, size_t value_len, acks_t acks, int *partition, long *offset){

    *partition = -1;
    *offset = -1;

    if(protocol_send_produce(client->file_descriptor, topic, (int)acks, key, key_len, value, value_len) < 0)
        return -1;


    char *body;
    size_t len;
    int status = read_reply(client, &body, &len);


    if(status == MB_OK && sscanf(body, "%d %ld", partition, offset) != 2)
        status = -1;

    free(body);
    return status;
}


//Broker replies "<generation> <p1,p2,...>" or "<generation> -"
int mbc_join(mb_client_t *client, const char *topic, const char *group, int from_beginning){
    char line[LINE_MAX_LEN];

    if(from_beginning)
        snprintf(line, sizeof line, "JOIN %s %s FROM_BEGINNING", topic, group);

    else
        snprintf(line, sizeof line, "JOIN %s %s", topic, group);

    char *body;
    size_t len;
    int status = mbc_request(client, line, &body, &len);

    if(status == MB_OK){
        if(sscanf(body, "%ld", &client->generation) != 1)
            status = -1;

        else{
            snprintf(client->topic, sizeof client->topic, "%s", topic);
            snprintf(client->group, sizeof client->group, "%s", group);
        }
    }

    free(body);
    return status;
}



int mbc_commit(mb_client_t *client, int partition, long offset){

    if(client->generation < 0)
        return MB_ERR_REBALANCE;   
    char line[LINE_MAX_LEN];
    snprintf(line, sizeof line, "COMMIT %s %s %ld %d %ld", client->topic, client->group, client->generation, partition, offset);

    char *body;
    size_t len;
    int status = mbc_request(client, line, &body, &len);

    free(body);
    return status;
}


int mbc_leave(mb_client_t *client){

    if(client->generation < 0)
        return MB_OK;  

    char line[LINE_MAX_LEN];
    snprintf(line, sizeof line, "LEAVE %s %s", client->topic, client->group);

    char *body;
    size_t len;
    int status = mbc_request(client, line, &body, &len);

    if(status == MB_OK)
        client->generation = -1;


    free(body);
    return status;
}


static const char *next_line(const char *body, size_t len, size_t *pos){
    const char *start = body + *pos;
    const char *end = memchr(start, '\n', len - *pos);

    if(end == NULL)
        return NULL;

    *pos = (size_t)(end - body) + 1;
    return start;
}



//Broker replies "<partition> <count>\n", then "<offset> <key_len> <value_len>\n<key><value>" 
int mbc_poll(mb_client_t *client, size_t max_bytes, int wait_ms, message_t **msgs, size_t *n, int *partition){

    *msgs = NULL;
    *n = 0;
    *partition = -1;

    if(client->generation < 0)
        return MB_ERR_REBALANCE;   

    char line[LINE_MAX_LEN];
    snprintf(line, sizeof line, "POLL %s %s %zu %d", client->topic, client->group, max_bytes, wait_ms);

    char *body;
    size_t len;
    int status = mbc_request(client, line, &body, &len);

    
    if(status != MB_OK){
        free(body);
        return status;
    }


    size_t pos = 0;
    size_t count;
    const char *header = next_line(body, len, &pos);


    if(header == NULL || sscanf(header, "%d %zu", partition, &count) != 2 || count > len){
        free(body);
        *partition = -1;
        return -1;
    }

    
    message_t *out = malloc(count * sizeof *out + len);

    if(out == NULL){
        free(body);
        *partition = -1;
        return -1;
    }

    char *bytes = (char *)(out + count);
    memcpy(bytes, body, len);


    for(size_t i = 0; i < count; i++){
        const char *text = next_line(body, len, &pos);
        long offset;
        unsigned key_len;
        unsigned value_len;

        //cut or malformed reply
        if(text == NULL || sscanf(text, "%ld %u %u", &offset, &key_len, &value_len) != 3 || len - pos < (size_t)key_len + value_len){
            free(out);
            free(body);
            *partition = -1;
            return -1;
        }

        out[i].offset = offset;
        out[i].timestamp_ms = 0;
        out[i].key_len = key_len;
        out[i].value_len = value_len;
        out[i].key = NULL;

        if(key_len > 0)
            out[i].key = bytes + pos;

            
        pos += key_len;
        out[i].value = bytes + pos;
        pos += value_len;
    }

    free(body);
    *msgs = out;
    *n = count;
    return MB_OK;

}