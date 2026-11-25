// Unit tests of the MPI layer with toy messages, no broker and no logs.
// Run with at least 2 processes: mpirun -np 3 ./bin/test_mpi_layer
//
// Every rank checks its own part and the results are combined with MPI_Allreduce,
// so the exit code is the same everywhere.

#define _POSIX_C_SOURCE 200809L

#include "mpi_layer.h"
#include "common.h"

#include <mpi.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// The real receiver asks the replication module whether the broker is paused.
// These tests never pause, so a stub keeps the replication module out.
static atomic_bool g_fake_paused = false;
bool repl_is_paused(void)
{
    return atomic_load(&g_fake_paused);
}

#define TAG_ECHO  40 // request: replies with the same bytes
#define TAG_COUNT 41 // one-way: counts messages and bytes
#define TAG_SLOW  42 // request that is never answered (timeout test)
#define TAG_BIG   43 // request: replies with a checksum of a large payload

#define THREADS 8
#define REQUESTS_PER_THREAD 200
#define ONE_WAY_MESSAGES 1000
#define BIG_BYTES (4u * 1024u * 1024u)

static int g_rank;
static int g_size;
static int g_failures;
static int g_checks;

static atomic_long g_count_messages = 0;
static atomic_long g_count_bytes = 0;
static pthread_mutex_t g_count_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_count_cond = PTHREAD_COND_INITIALIZER;

static void check(bool ok, const char *what)
{
    g_checks++;
    if (!ok) {
        g_failures++;
        fprintf(stderr, "  ✘ rank %d: %s\n", g_rank, what);
    }
}

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static uint64_t fnv64(const unsigned char *p, size_t n)
{
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= 1099511628211ull;
    }
    return h;
}

// Handlers run on the receiver thread. Requests carry an 8-byte request id first.
static void on_echo(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    uint64_t id;
    memcpy(&id, buf, sizeof id);
    mpil_reply(source, id, (const char *)buf + sizeof id, len - sizeof id);
}

static void on_count(int source, const void *buf, size_t len, void *arg)
{
    (void)source;
    (void)buf;
    (void)arg;
    pthread_mutex_lock(&g_count_mutex);
    atomic_fetch_add(&g_count_messages, 1);
    atomic_fetch_add(&g_count_bytes, (long)len);
    pthread_cond_broadcast(&g_count_cond);
    pthread_mutex_unlock(&g_count_mutex);
}

static void on_slow(int source, const void *buf, size_t len, void *arg)
{
    (void)source;
    (void)buf;
    (void)len;
    (void)arg; // never answers
}

static void on_big(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    uint64_t id;
    memcpy(&id, buf, sizeof id);
    uint64_t sum = fnv64((const unsigned char *)buf + sizeof id, len - sizeof id);
    mpil_reply(source, id, &sum, sizeof sum);
}

typedef struct {
    int thread;
    int errors;
} worker_t;

// Many threads send requests at the same time to every other rank (MPI_THREAD_MULTIPLE)
// and each reply must come back to the thread that asked, with its own payload.
static void *request_worker(void *arg)
{
    worker_t *w = arg;
    for (int i = 0; i < REQUESTS_PER_THREAD; i++) {
        int dest = (g_rank + 1 + (i % (g_size - 1))) % g_size;
        char req[64];
        int n = snprintf(req, sizeof req, "rank=%d thread=%d i=%d", g_rank, w->thread, i);
        void *reply = NULL;
        size_t reply_len = 0;
        int rc = mpil_request(dest, TAG_ECHO, req, (size_t)n, &reply, &reply_len, 5000);
        if (rc != MB_OK || reply_len != (size_t)n || memcmp(reply, req, (size_t)n) != 0) {
            w->errors++;
        }
        free(reply);
    }
    return NULL;
}

typedef struct {
    int rc;
    double waited_ms;
} blocked_t;

static void *blocked_request(void *arg)
{
    blocked_t *b = arg;
    void *reply = NULL;
    size_t reply_len = 0;
    double t0 = now_ms();
    b->rc = mpil_request((g_rank + 1) % g_size, TAG_SLOW, "x", 1, &reply, &reply_len, 30000);
    b->waited_ms = now_ms() - t0;
    free(reply);
    return NULL;
}

int main(int argc, char **argv)
{
    if (mpil_init(&argc, &argv, &g_rank, &g_size) != 0) {
        fprintf(stderr, "mpil_init failed\n");
        return EXIT_FAILURE;
    }
    if (g_size < 2) {
        if (g_rank == 0) {
            fprintf(stderr, "test_mpi_layer needs at least 2 MPI processes\n");
        }
        mpil_shutdown();
        return EXIT_FAILURE;
    }
    check(mpil_rank() == g_rank && mpil_size() == g_size, "rank and size are reported");

    mpil_register(TAG_ECHO, on_echo, NULL);
    mpil_register(TAG_COUNT, on_count, NULL);
    mpil_register(TAG_SLOW, on_slow, NULL);
    mpil_register(TAG_BIG, on_big, NULL);
    mpil_barrier(); // every handler is registered before anyone sends

    // 1. Concurrent request/reply from several threads to every other rank.
    pthread_t threads[THREADS];
    worker_t workers[THREADS];
    for (int t = 0; t < THREADS; t++) {
        workers[t] = (worker_t){ .thread = t, .errors = 0 };
        pthread_create(&threads[t], NULL, request_worker, &workers[t]);
    }
    int errors = 0;
    for (int t = 0; t < THREADS; t++) {
        pthread_join(threads[t], NULL);
        errors += workers[t].errors;
    }
    check(errors == 0, "every concurrent request got its own reply");

    // 2. One-way messages: each rank sends ONE_WAY_MESSAGES to every other rank and
    //    waits on a condition variable until it received all of its own.
    for (int i = 0; i < ONE_WAY_MESSAGES; i++) {
        for (int r = 0; r < g_size; r++) {
            if (r != g_rank) {
                char msg[16] = { 0 };
                while (mpil_send(r, TAG_COUNT, msg, sizeof msg) != 0) {
                    // Send queue full: wait until it drains.
                    mpil_drain_sends(1000);
                }
            }
        }
    }
    long expected = (long)ONE_WAY_MESSAGES * (g_size - 1);
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 20;
    pthread_mutex_lock(&g_count_mutex);
    while (atomic_load(&g_count_messages) < expected) {
        if (pthread_cond_timedwait(&g_count_cond, &g_count_mutex, &deadline) != 0) {
            break;
        }
    }
    pthread_mutex_unlock(&g_count_mutex);
    check(atomic_load(&g_count_messages) == expected, "every one-way message arrived exactly once");
    check(atomic_load(&g_count_bytes) == expected * 16, "one-way payload sizes are intact");

    // 3. A large message (4 MiB) goes through intact.
    unsigned char *big = malloc(BIG_BYTES);
    for (size_t i = 0; i < BIG_BYTES; i++) {
        big[i] = (unsigned char)((i * 131u + (unsigned)g_rank) & 0xFF);
    }
    void *reply = NULL;
    size_t reply_len = 0;
    int rc = mpil_request((g_rank + 1) % g_size, TAG_BIG, big, BIG_BYTES, &reply, &reply_len, 10000);
    uint64_t sum = 0;
    if (reply_len == sizeof sum) {
        memcpy(&sum, reply, sizeof sum);
    }
    check(rc == MB_OK && sum == fnv64(big, BIG_BYTES), "a 4 MiB request arrives intact");
    free(reply);
    free(big);

    // 4. A request nobody answers ends with MB_ERR_TIMEOUT close to its deadline.
    double t0 = now_ms();
    rc = mpil_request((g_rank + 1) % g_size, TAG_SLOW, "x", 1, &reply, &reply_len, 300);
    double waited = now_ms() - t0;
    check(rc == MB_ERR_TIMEOUT, "unanswered request times out");
    check(waited >= 280 && waited < 2000, "timeout honours its deadline");

    // 5. mpil_drain_sends returns once the queue is empty.
    check(mpil_drain_sends(5000) == 0, "send queue drains");

    mpil_barrier();

    // 6. mpil_abort_requests wakes a thread blocked in mpil_request long before
    //    its 30 s deadline, and later requests fail at once (shutdown path).
    blocked_t blocked = { 0 };
    pthread_t blocked_thread;
    pthread_create(&blocked_thread, NULL, blocked_request, &blocked);
    struct timespec pause_ts = { 0, 200 * 1000000L };
    nanosleep(&pause_ts, NULL); // let it block first
    mpil_abort_requests();
    pthread_join(blocked_thread, NULL);
    check(blocked.rc == MB_ERR_SHUTTING_DOWN, "aborted request reports shutting down");
    check(blocked.waited_ms < 5000, "abort wakes the blocked request promptly");
    rc = mpil_request((g_rank + 1) % g_size, TAG_ECHO, "y", 1, &reply, &reply_len, 5000);
    check(rc != MB_OK, "requests fail once aborted");
    free(reply);

    // Combine the results before shutting down.
    int local[2] = { g_failures, g_checks };
    int total[2] = { 0, 0 };
    MPI_Allreduce(local, total, 2, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    // 7. Ordered shutdown: drain, barrier, STOP to the receiver, join, finalize.
    mpil_drain_sends(2000);
    mpil_barrier();
    mpil_shutdown();

    if (g_rank == 0) {
        if (total[0] == 0) {
            printf("  ✔ test_mpi_layer: %d checks passed on %d ranks\n", total[1], g_size);
        } else {
            printf("  ✘ test_mpi_layer: %d of %d checks failed\n", total[0], total[1]);
        }
    }
    return total[0] == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
