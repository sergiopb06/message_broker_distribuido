#ifndef LOG_H
#define LOG_H

#include "common.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define PLOG_MAX_MESSAGE_BYTES (1u << 20)
#define PLOG_DEFAULT_SEGMENT_BYTES (1ull << 26)
#define PLOG_MIN_SEGMENT_BYTES (1ull << 12)

typedef struct partition_log partition_log_t;

// Reopens an existing directory: every record is validated, an incomplete tail of the last
// segment is dropped, and a corrupt earlier segment or a gap between segments fails with EIO.
// The high watermark is restored from a checkpoint written by plog_flush and plog_close.
// retention_bytes = 0 means no limit. Above the limit, whole segments are dropped oldest first,
// but never the active segment nor data at or above the high watermark. Offsets are never reused.
// A reader asking for an offset that retention already dropped continues from the first one left.
// plog_open sizes the segments from the limit (retention_bytes / 4, at least
// PLOG_MIN_SEGMENT_BYTES, at most PLOG_DEFAULT_SEGMENT_BYTES); plog_open_ex takes the size.
partition_log_t *plog_open(const char *dir, uint64_t retention_bytes);
partition_log_t *plog_open_ex(const char *dir, uint64_t retention_bytes, uint64_t segment_bytes);

int plog_append(partition_log_t *log, const message_t *msgs, size_t n, int64_t *base_offset);
int plog_append_replica(partition_log_t *log, const message_t *msgs, size_t n);

// Offsets below the start offset are read from the start offset.
// The result is one allocation: release it with plog_free_messages.
int plog_read(partition_log_t *log, int64_t from, size_t max_bytes, bool committed_only,
              message_t **out, size_t *n);
void plog_free_messages(message_t *msgs);

// timeout_ms < 0 waits forever. Returns MB_OK, MB_ERR_TIMEOUT or MB_ERR_SHUTTING_DOWN.
int plog_wait_data(partition_log_t *log, int64_t from, bool committed_only, int timeout_ms);

int64_t plog_start_offset(partition_log_t *log);
int64_t plog_end_offset(partition_log_t *log);
int64_t plog_hwm(partition_log_t *log);
void plog_set_hwm(partition_log_t *log, int64_t hwm);

int plog_flush(partition_log_t *log);

// Wakes every waiter and frees the log once no thread is using it.
void plog_close(partition_log_t *log);

#endif