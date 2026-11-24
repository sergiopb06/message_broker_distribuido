#ifndef REPLICATION_H
#define REPLICATION_H

#include "common.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef enum {
    ADMIN_PAUSE,
    ADMIN_RESUME,
    ADMIN_SHUTDOWN
} admin_cmd_t;

int repl_init(int rank, int size);
int repl_start(void);
void repl_shutdown(void);

// Waits (condition variable, with a deadline) until the message at `offset`
// is replicated according to `acks`. acks 0 and 1 return immediately.
int repl_wait_acks(const char *topic, int partition, int64_t offset, acks_t acks, int timeout_ms);

bool repl_is_leader(const char *topic, int partition);

// Leader view of a partition (ISR, HWM and LEO), asking the leader over MPI
// when this broker is not the leader.
int repl_partition_state(const char *topic, int partition, int *isr, int *n_isr, int64_t *hwm, int64_t *leo);
int repl_get_isr(const char *topic, int partition, int *isr, int *n_isr);

// Offsets of a partition this broker keeps, or the leader's if it does not.
int repl_partition_offsets(const char *topic, int partition, int64_t *start, int64_t *hwm, int64_t *leo);

// Consumer read: local when the log is here, from the leader otherwise.
// Only committed messages (below the HWM) are returned.
int repl_read(const char *topic, int partition, int64_t from, size_t max_bytes,
              message_t **out, size_t *n);

// Blocks (no busy wait) until the partition has data past `from`.
int repl_wait_data(const char *topic, int partition, int64_t from, int timeout_ms);

// Sends a produce to the partition leader over MPI.
int repl_forward_produce(const char *topic, int partition, const char *key, size_t key_len,
                         const char *value, size_t value_len, acks_t acks, int64_t *offset);

int cluster_admin(int broker_id, admin_cmd_t command);

bool repl_is_paused(void);
void repl_set_paused(bool paused);
void repl_wait_if_paused(void);

int repl_forward_topic_create(const char *name, int partitions, int replication, uint64_t retention_bytes);
void repl_broadcast_metadata(void);
int repl_request_metadata(void);

#endif // REPLICATION_H
