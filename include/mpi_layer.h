#ifndef MPI_LAYER_H
#define MPI_LAYER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#define MPIL_TAG_STOP         1
#define MPIL_TAG_ADMIN        2
#define MPIL_TAG_FETCH_REQ    3
#define MPIL_TAG_FETCH_RESP   4
#define MPIL_TAG_HEARTBEAT    5
#define MPIL_TAG_FORWARD_REQ  6
#define MPIL_TAG_FORWARD_RESP 7
#define MPIL_TAG_META_DIFFUSE 8
#define MPIL_TAG_GROUP_GONE   9
#define MPIL_TAG_STATE_REQ    10
#define MPIL_TAG_READ_REQ     11
#define MPIL_TAG_META_REQ     12
#define MPIL_TAG_PRODUCE_REQ  13

typedef void (*mpil_handler_t)(int source, const void *buf, size_t len, void *arg);

int mpil_init(int *argc, char ***argv, int *rank, int *size);
int mpil_rank(void);
int mpil_size(void);
void mpil_register(int tag, mpil_handler_t handler, void *arg);
int mpil_send(int dest, int tag, const void *buf, size_t len);
int mpil_reply(int dest, uint64_t req_id, const void *buf, size_t len);
int mpil_request(int dest, int tag, const void *req, size_t len,
                 void **reply, size_t *reply_len, int timeout_ms);

// Wakes every thread blocked in mpil_request and makes new requests fail at once.
// Used by the ordered shutdown so no worker waits for a reply that will never come.
void mpil_abort_requests(void);

int mpil_barrier(void);
void mpil_shutdown(void);

#endif // MPI_LAYER_H
