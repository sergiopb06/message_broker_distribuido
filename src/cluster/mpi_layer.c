#define _POSIX_C_SOURCE 200809L

#include "mpi_layer.h"
#include "replication.h"
#include "common.h"

#include <mpi.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

#define MAX_TAGS 128

typedef struct pending_req {
    uint64_t req_id;
    pthread_cond_t cond;
    void *reply;
    size_t reply_len;
    bool completed;
    struct pending_req *next;
} pending_req_t;

typedef struct {
    mpil_handler_t handler;
    void *arg;
} tag_handler_t;

typedef struct send_job {
    int dest;
    int tag;
    size_t len;
    struct send_job *next;
    char data[];
} send_job_t;

#define SEND_QUEUE_MAX_JOBS 4096
#define SEND_QUEUE_MAX_BYTES (128u * 1024u * 1024u)

static int g_rank = 0;
static int g_size = 1;
static bool g_initialized = false;
static atomic_bool g_stopping = false;
static atomic_bool g_abort_requests = false;

static pthread_t g_recv_thread;
static pthread_t g_send_thread;
static pthread_mutex_t g_pending_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_handler_mutex = PTHREAD_MUTEX_INITIALIZER;

// Sends are queued and drained by one dedicated thread. The MPI receiver must
// never block inside MPI_Send: with large messages it would wait for the peer
// to post a matching receive, while the peer's receiver could be doing the same.
static pthread_mutex_t g_send_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_send_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t g_send_drained = PTHREAD_COND_INITIALIZER;
static send_job_t *g_send_head = NULL;
static send_job_t *g_send_tail = NULL;
static size_t g_send_count = 0;
static size_t g_send_bytes = 0;
static bool g_send_active = false;
static bool g_send_closed = false;
static bool g_send_started = false;

static pending_req_t *g_pending_head = NULL;
static atomic_uint_fast64_t g_next_req_id = 1;

static tag_handler_t g_handlers[MAX_TAGS];

static void *sender_thread(void *arg)
{
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&g_send_mutex);
        while (g_send_head == NULL && !g_send_closed) {
            pthread_cond_wait(&g_send_cond, &g_send_mutex);
        }
        if (g_send_head == NULL && g_send_closed) {
            pthread_mutex_unlock(&g_send_mutex);
            break;
        }
        send_job_t *job = g_send_head;
        g_send_head = job->next;
        if (g_send_head == NULL) {
            g_send_tail = NULL;
        }
        g_send_count--;
        g_send_bytes -= job->len;
        g_send_active = true;
        pthread_mutex_unlock(&g_send_mutex);

        (void)MPI_Send(job->data, (int)job->len, MPI_BYTE, job->dest, job->tag,
                       MPI_COMM_WORLD);
        free(job);

        pthread_mutex_lock(&g_send_mutex);
        g_send_active = false;
        pthread_cond_broadcast(&g_send_drained);
        pthread_mutex_unlock(&g_send_mutex);
    }
    return NULL;
}

static void *receiver_thread(void *arg)
{
    (void)arg;
    while (!atomic_load(&g_stopping)) {
        MPI_Status status;
        int rc = MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (rc != MPI_SUCCESS || atomic_load(&g_stopping)) {
            break;
        }

        int count = 0;
        MPI_Get_count(&status, MPI_BYTE, &count);
        if (count < 0) {
            count = 0;
        }

        char *buf = NULL;
        if (count > 0) {
            buf = malloc((size_t)count);
            if (buf == NULL) {
                // Out of memory; consume message to avoid deadlock
                char dummy[64];
                while (count > 0) {
                    int chunk = (count > (int)sizeof(dummy)) ? (int)sizeof(dummy) : count;
                    MPI_Recv(dummy, chunk, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
                    count -= chunk;
                }
                continue;
            }
        }

        rc = MPI_Recv(buf, count, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
        if (rc != MPI_SUCCESS) {
            free(buf);
            break;
        }

        if (status.MPI_TAG == MPIL_TAG_STOP) {
            free(buf);
            break;
        }

        if (status.MPI_TAG == MPIL_TAG_FORWARD_RESP) {
            if ((size_t)count >= sizeof(uint64_t)) {
                uint64_t req_id = 0;
                memcpy(&req_id, buf, sizeof(uint64_t));
                size_t payload_len = (size_t)count - sizeof(uint64_t);

                pthread_mutex_lock(&g_pending_mutex);
                pending_req_t *curr = g_pending_head;
                while (curr != NULL) {
                    if (curr->req_id == req_id) {
                        if (payload_len > 0) {
                            curr->reply = malloc(payload_len);
                            if (curr->reply != NULL) {
                                memcpy(curr->reply, buf + sizeof(uint64_t), payload_len);
                                curr->reply_len = payload_len;
                            } else {
                                curr->reply_len = 0;
                            }
                        } else {
                            curr->reply = NULL;
                            curr->reply_len = 0;
                        }
                        curr->completed = true;
                        pthread_cond_signal(&curr->cond);
                        break;
                    }
                    curr = curr->next;
                }
                pthread_mutex_unlock(&g_pending_mutex);
            }
            free(buf);
            continue;
        }

        // Paused broker: it answers neither clients nor brokers. Admin commands
        // (in particular RESUME) and metadata diffusion are the only exceptions,
        // so a paused broker can be resumed and does not miss new topics.
        if (repl_is_paused() && status.MPI_TAG != MPIL_TAG_ADMIN
            && status.MPI_TAG != MPIL_TAG_META_DIFFUSE) {
            free(buf);
            continue;
        }

        int tag = status.MPI_TAG;
        mpil_handler_t handler = NULL;
        void *handler_arg = NULL;

        if (tag >= 0 && tag < MAX_TAGS) {
            pthread_mutex_lock(&g_handler_mutex);
            handler = g_handlers[tag].handler;
            handler_arg = g_handlers[tag].arg;
            pthread_mutex_unlock(&g_handler_mutex);
        }

        if (handler != NULL) {
            handler(status.MPI_SOURCE, buf, (size_t)count, handler_arg);
        }

        free(buf);
    }
    return NULL;
}

int mpil_init(int *argc, char ***argv, int *rank, int *size)
{
    if (g_initialized) {
        *rank = g_rank;
        *size = g_size;
        return 0;
    }

    int provided = 0;
    int rc = MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
    if (rc != MPI_SUCCESS) {
        fprintf(stderr, "MPI_Init_thread failed\n");
        return -1;
    }

    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI_THREAD_MULTIPLE not provided (level %d)\n", provided);
        MPI_Finalize();
        return -1;
    }

    // A peer that already finished must not abort this process on a late send.
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &g_size);
    *rank = g_rank;
    *size = g_size;

    g_initialized = true;
    atomic_store(&g_stopping, false);
    atomic_store(&g_abort_requests, false);

    pthread_mutex_lock(&g_send_mutex);
    g_send_head = NULL;
    g_send_tail = NULL;
    g_send_count = 0;
    g_send_bytes = 0;
    g_send_active = false;
    g_send_closed = false;
    pthread_mutex_unlock(&g_send_mutex);

    if (pthread_create(&g_send_thread, NULL, sender_thread, NULL) != 0) {
        perror("pthread_create sender_thread");
        MPI_Finalize();
        g_initialized = false;
        return -1;
    }
    g_send_started = true;

    if (pthread_create(&g_recv_thread, NULL, receiver_thread, NULL) != 0) {
        perror("pthread_create receiver_thread");
        pthread_mutex_lock(&g_send_mutex);
        g_send_closed = true;
        pthread_cond_broadcast(&g_send_cond);
        pthread_mutex_unlock(&g_send_mutex);
        pthread_join(g_send_thread, NULL);
        g_send_started = false;
        MPI_Finalize();
        g_initialized = false;
        return -1;
    }

    return 0;
}

int mpil_rank(void)
{
    return g_rank;
}

int mpil_size(void)
{
    return g_size;
}

void mpil_register(int tag, mpil_handler_t handler, void *arg)
{
    if (tag < 0 || tag >= MAX_TAGS) {
        return;
    }
    pthread_mutex_lock(&g_handler_mutex);
    g_handlers[tag].handler = handler;
    g_handlers[tag].arg = arg;
    pthread_mutex_unlock(&g_handler_mutex);
}

int mpil_send(int dest, int tag, const void *buf, size_t len)
{
    if (len > 0 && buf == NULL) {
        return -1;
    }
    if (len > (size_t)INT_MAX) {
        return -1;
    }

    send_job_t *job = malloc(sizeof *job + len);
    if (job == NULL) {
        return -1;
    }
    job->dest = dest;
    job->tag = tag;
    job->len = len;
    job->next = NULL;
    if (len > 0) {
        memcpy(job->data, buf, len);
    }

    pthread_mutex_lock(&g_send_mutex);
    if (g_send_closed || g_send_count >= SEND_QUEUE_MAX_JOBS
        || g_send_bytes + len > SEND_QUEUE_MAX_BYTES) {
        pthread_mutex_unlock(&g_send_mutex);
        free(job);
        return -1;
    }
    if (g_send_tail == NULL) {
        g_send_head = job;
    } else {
        g_send_tail->next = job;
    }
    g_send_tail = job;
    g_send_count++;
    g_send_bytes += len;
    pthread_cond_signal(&g_send_cond);
    pthread_mutex_unlock(&g_send_mutex);
    return 0;
}

int mpil_reply(int dest, uint64_t req_id, const void *buf, size_t len)
{
    size_t total = sizeof(uint64_t) + len;
    char *msg = malloc(total);
    if (msg == NULL) {
        return -1;
    }
    memcpy(msg, &req_id, sizeof(uint64_t));
    if (len > 0 && buf != NULL) {
        memcpy(msg + sizeof(uint64_t), buf, len);
    }
    int rc = mpil_send(dest, MPIL_TAG_FORWARD_RESP, msg, total);
    free(msg);
    return rc;
}

int mpil_request(int dest, int tag, const void *req, size_t len,
                 void **reply, size_t *reply_len, int timeout_ms)
{
    if (reply == NULL || reply_len == NULL) {
        return -1;
    }
    *reply = NULL;
    *reply_len = 0;

    pending_req_t preq;
    memset(&preq, 0, sizeof(preq));
    preq.req_id = atomic_fetch_add(&g_next_req_id, 1);
    preq.completed = false;
    pthread_cond_init(&preq.cond, NULL);

    pthread_mutex_lock(&g_pending_mutex);
    preq.next = g_pending_head;
    g_pending_head = &preq;
    pthread_mutex_unlock(&g_pending_mutex);

    size_t total = sizeof(uint64_t) + len;
    char *msg = malloc(total);
    if (msg == NULL) {
        pthread_mutex_lock(&g_pending_mutex);
        if (g_pending_head == &preq) {
            g_pending_head = preq.next;
        } else {
            pending_req_t *p = g_pending_head;
            while (p != NULL && p->next != &preq) {
                p = p->next;
            }
            if (p != NULL) p->next = preq.next;
        }
        pthread_mutex_unlock(&g_pending_mutex);
        pthread_cond_destroy(&preq.cond);
        return -1;
    }

    memcpy(msg, &preq.req_id, sizeof(uint64_t));
    if (len > 0 && req != NULL) {
        memcpy(msg + sizeof(uint64_t), req, len);
    }

    if (mpil_send(dest, tag, msg, total) != 0) {
        free(msg);
        pthread_mutex_lock(&g_pending_mutex);
        if (g_pending_head == &preq) {
            g_pending_head = preq.next;
        } else {
            pending_req_t *p = g_pending_head;
            while (p != NULL && p->next != &preq) {
                p = p->next;
            }
            if (p != NULL) p->next = preq.next;
        }
        pthread_mutex_unlock(&g_pending_mutex);
        pthread_cond_destroy(&preq.cond);
        return -1;
    }
    free(msg);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_ms / 1000;
    ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&g_pending_mutex);
    while (!preq.completed && !atomic_load(&g_stopping) && !atomic_load(&g_abort_requests)) {
        int rc = pthread_cond_timedwait(&preq.cond, &g_pending_mutex, &ts);
        if (rc == ETIMEDOUT) {
            break;
        }
    }

    // Unlink from list
    if (g_pending_head == &preq) {
        g_pending_head = preq.next;
    } else {
        pending_req_t *p = g_pending_head;
        while (p != NULL && p->next != &preq) {
            p = p->next;
        }
        if (p != NULL) {
            p->next = preq.next;
        }
    }
    pthread_mutex_unlock(&g_pending_mutex);
    pthread_cond_destroy(&preq.cond);

    if (preq.completed) {
        *reply = preq.reply;
        *reply_len = preq.reply_len;
        return MB_OK;
    }

    if (atomic_load(&g_stopping) || atomic_load(&g_abort_requests)) {
        return MB_ERR_SHUTTING_DOWN;
    }
    return MB_ERR_TIMEOUT;
}

void mpil_abort_requests(void)
{
    atomic_store(&g_abort_requests, true);
    pthread_mutex_lock(&g_pending_mutex);
    pending_req_t *p = g_pending_head;
    while (p != NULL) {
        pthread_cond_signal(&p->cond);
        p = p->next;
    }
    pthread_mutex_unlock(&g_pending_mutex);
}

int mpil_drain_sends(int timeout_ms)
{
    if (!g_initialized) {
        return 0;
    }

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += timeout_ms / 1000;
    deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }

    int rc = 0;
    pthread_mutex_lock(&g_send_mutex);
    while (g_send_count > 0 || g_send_active) {
        int w = pthread_cond_timedwait(&g_send_drained, &g_send_mutex, &deadline);
        if (w == ETIMEDOUT) {
            rc = MB_ERR_TIMEOUT;
            break;
        }
    }
    pthread_mutex_unlock(&g_send_mutex);
    return rc;
}

int mpil_barrier(void)
{
    return (MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS) ? 0 : -1;
}

void mpil_shutdown(void)
{
    if (!g_initialized || atomic_load(&g_stopping)) {
        return;
    }
    atomic_store(&g_stopping, true);

    // Wake receiver thread with STOP tag
    int dummy = 0;
    (void)mpil_send(g_rank, MPIL_TAG_STOP, &dummy, sizeof(dummy));

    pthread_join(g_recv_thread, NULL);

    pthread_mutex_lock(&g_pending_mutex);
    pending_req_t *p = g_pending_head;
    while (p != NULL) {
        pthread_cond_signal(&p->cond);
        p = p->next;
    }
    pthread_mutex_unlock(&g_pending_mutex);

    // The sender already delivered STOP. Anything still queued was enqueued after
    // the shutdown barrier, for peers that may have finalized already: drop it
    // rather than risk a send nobody receives. Then join the sender, so no thread
    // can be inside MPI_Send when MPI_Finalize runs.
    pthread_mutex_lock(&g_send_mutex);
    while (g_send_head != NULL) {
        send_job_t *job = g_send_head;
        g_send_head = job->next;
        free(job);
    }
    g_send_tail = NULL;
    g_send_count = 0;
    g_send_bytes = 0;
    g_send_closed = true;
    pthread_cond_broadcast(&g_send_cond);
    pthread_mutex_unlock(&g_send_mutex);
    if (g_send_started) {
        pthread_join(g_send_thread, NULL);
        g_send_started = false;
    }

    MPI_Finalize();
    g_initialized = false;
}
