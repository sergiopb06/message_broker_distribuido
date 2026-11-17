#ifndef COMMON_H
#define COMMON_H

#include <stdint.h>

typedef enum { ACKS_NONE = 0, ACKS_LEADER = 1, ACKS_ALL = -1 } acks_t;

typedef enum {
    MB_OK,
    MB_ERR_UNKNOWN_TOPIC,
    MB_ERR_TOPIC_EXISTS,
    MB_ERR_NOT_LEADER,
    MB_ERR_OFFSET_OUT_OF_RANGE,
    MB_ERR_REBALANCE,
    MB_ERR_TIMEOUT,
    MB_ERR_SHUTTING_DOWN,
    MB_ERR_INVALID_ARGUMENT
} mb_status_t;

typedef struct {
    int64_t offset;
    int64_t timestamp_ms;
    uint32_t key_len; // 0 means no key
    uint32_t value_len;
    char *key;
    char *value;
} message_t;

#endif