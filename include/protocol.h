
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stddef.h>

#define TOPIC_NAME_MAX 64
#define GROUP_NAME_MAX 64

#define MAX_KEY_BYTES 1024 //so a client can't send a giant key and shutdown the broker.
#define MAX_VALUE_BYTES (1024 * 1024) //1MiB

#define MAX_RESPONSE_BYTES (16 *1024 * 1024) //16MiB

typedef enum {
    C_TOPIC_CREATE,
    C_TOPIC_LIST,
    C_TOPIC_DESCRIBE,
    C_PRODUCE,
    C_JOIN,
    C_POLL,
    C_COMMIT,
    C_LEAVE,
    C_GROUP_DESCRIBE,
    C_BROKER_PAUSE,
    C_BROKER_RESUME,
    C_SHUTDOWN,
    C_INVALID
} c_type_t;


typedef struct  {
    c_type_t type;
    char topic[TOPIC_NAME_MAX];
    char group[GROUP_NAME_MAX];
    int partitions;
    int replication;
    long retention_bytes;
    int acks;
    size_t key_len;
    size_t value_len;
    size_t max_bytes;
    int wait_ms;
    int broker_id;
    int from_beginning;
    long generation;
    int partition;
    long offset;
    char error[128];
} command_t;


int protocol_parse(const char *line, command_t *out);

int protocol_send_response(int file_descriptor, int ok, const char *body);

//send response with explicit length
int protocol_send_response_bytes(int file_descriptor, int ok, const char *body, size_t len);

int protocol_read_response(int file_descriptor, int *ok, char **body, size_t *len);

//sends PRODUCE line followed by the key and value bytes.
//key can be NULL
int protocol_send_produce(int file_descriptor, const char *topic, int acks, const char *key, size_t key_len, const char *value, size_t value_len);

// Reads one response and prints its body.
// 1 = OK, 0 = ERR, -1 = no response.
int protocol_print_response(int file_descriptor, size_t *len);

#endif //PROTOCOL_H