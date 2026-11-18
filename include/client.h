#ifndef CLIENT_H
#define CLIENT_H

#include "common.h"

#include <stddef.h>

//mbc = message broker client

//Connection to one broker.
typedef struct mb_client mb_client_t;


mb_client_t *mbc_connect(const char *host, unsigned short port);

void mbc_close(mb_client_t *client);

int mbc_request(mb_client_t *client, const char *line, char **body, size_t *len);

int mbc_produce(mb_client_t *client, const char *topic, const char *key, size_t key_len, const char *value, size_t value_len, acks_t acks, int *partition, long *offset);

int mbc_join(mb_client_t *client, const char *topic, const char *group, int from_beginning);

int mbc_poll(mb_client_t *client, size_t max_bytes, int wait_ms, message_t **msgs, size_t *n, int *partition);

int mbc_commit(mb_client_t *client, int partition, long offset);

int mbc_leave(mb_client_t *client);

#endif //CLIENT_H