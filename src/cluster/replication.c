#define _POSIX_C_SOURCE 200809L

#include "replication.h"
#include "mpi_layer.h"
#include "metadata.h"
#include "log.h"
#include "common.h"
#include "protocol.h"
#include "groups.h"
#include "net_util.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define HEARTBEAT_INTERVAL_MS 500
#define REPLICA_TIMEOUT_MS 3000.0
#define FETCH_CHUNK_BYTES (64 * 1024)
#define FETCH_IDLE_MS 20
#define STATE_TIMEOUT_MS 900
#define READ_TIMEOUT_MS 1200
#define PRODUCE_TIMEOUT_MS 7000
#define MAX_REMOTE_READ_BYTES (4u * 1024u * 1024u)

// Forwarded requests are processed outside the MPI receiver thread. A blocking
// JOIN or an acks=all wait must never stop us from answering FETCH requests.
#define FORWARD_WORKERS 16
#define FORWARD_QUEUE_MAX 512

extern int server_is_running(void);
extern void server_request_shutdown(void);

typedef struct {
    int rank;
    int64_t leo;
    double last_seen_ms;
    bool in_isr;
} replica_state_t;

typedef struct part_state {
    char topic[META_NAME_MAX];
    int partition;
    int leader;
    int n_replicas;
    int replicas[META_MAX_REPLICAS];
    replica_state_t replica_states[META_MAX_REPLICAS];
    pthread_mutex_t mutex;
    pthread_cond_t hwm_cond;
    struct part_state *next;
} part_state_t;

typedef struct {
    char topic[META_NAME_MAX];
    int partition;
    int64_t from_offset;
    int replica_rank;
} fetch_req_t;

typedef struct {
    admin_cmd_t cmd;
} admin_msg_t;

typedef struct {
    char topic[META_NAME_MAX];
    int partition;
    int acks;
    uint32_t key_len;
    uint32_t value_len;
} produce_req_t;

typedef struct {
    int status;
    int64_t offset;
} produce_resp_t;

typedef struct {
    char topic[META_NAME_MAX];
    int partition;
} state_req_t;

typedef struct {
    int status;
    int64_t start;
    int64_t hwm;
    int64_t leo;
    int32_t n_isr;
    int32_t isr[META_MAX_REPLICAS];
} state_resp_t;

typedef struct {
    char topic[META_NAME_MAX];
    int partition;
    int64_t from;
    uint64_t max_bytes;
} read_req_t;

typedef struct {
    int status;
    int64_t start;
    int64_t hwm;
    int64_t leo;
    uint32_t count;
} read_resp_t;

typedef struct forward_job {
    int tag;
    int source;
    size_t len;
    struct forward_job *next;
    char data[];
} forward_job_t;

static int g_rank = 0;
static int g_size = 1;
static bool g_repl_started = false;
static atomic_bool g_repl_stopping = false;

static bool g_paused = false;
static pthread_mutex_t g_pause_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_pause_cond = PTHREAD_COND_INITIALIZER;

// Set when a paused broker is resumed: it may have missed topic creations and
// asks the controller for a fresh metadata snapshot.
static atomic_bool g_need_metadata = false;

static double g_peer_last_seen_ms[128];
static pthread_mutex_t g_peer_mutex = PTHREAD_MUTEX_INITIALIZER;

static part_state_t *g_part_states = NULL;
static pthread_mutex_t g_part_states_mutex = PTHREAD_MUTEX_INITIALIZER;

static pthread_t g_fetch_thread;
static pthread_t g_heartbeat_thread;

static pthread_mutex_t g_fetch_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_fetch_cond = PTHREAD_COND_INITIALIZER;

// Wakes threads that wait for data hosted on another broker. Every heartbeat
// and every completed fetch advances the tick, so no thread polls in a loop.
static pthread_mutex_t g_tick_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_tick_cond = PTHREAD_COND_INITIALIZER;
static uint64_t g_tick = 0;

static pthread_mutex_t g_fwd_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_fwd_cond = PTHREAD_COND_INITIALIZER;
static forward_job_t *g_fwd_head = NULL;
static forward_job_t *g_fwd_tail = NULL;
static size_t g_fwd_count = 0;
static bool g_fwd_closed = false;
static pthread_t g_fwd_threads[FORWARD_WORKERS];
static bool g_fwd_started = false;

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

// A peer that stopped sending heartbeats is considered down (a paused broker
// behaves exactly like a crashed one). Requests to it fail fast instead of
// waiting for a reply that will never arrive.
static bool peer_alive(int rank)
{
    if (rank == g_rank) {
        return true;
    }
    if (rank < 0 || rank >= 128) {
        return false;
    }
    pthread_mutex_lock(&g_peer_mutex);
    double last = g_peer_last_seen_ms[rank];
    pthread_mutex_unlock(&g_peer_mutex);
    return (now_ms() - last) <= REPLICA_TIMEOUT_MS;
}

static void tick_advance(void)
{
    pthread_mutex_lock(&g_tick_mutex);
    g_tick++;
    pthread_cond_broadcast(&g_tick_cond);
    pthread_mutex_unlock(&g_tick_mutex);
}

static uint64_t tick_value(void)
{
    pthread_mutex_lock(&g_tick_mutex);
    uint64_t value = g_tick;
    pthread_mutex_unlock(&g_tick_mutex);
    return value;
}

static void tick_wait(uint64_t observed, int timeout_ms)
{
    if (timeout_ms <= 0) {
        return;
    }
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += timeout_ms / 1000;
    deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&g_tick_mutex);
    while (g_tick == observed && !atomic_load(&g_repl_stopping)) {
        if (pthread_cond_timedwait(&g_tick_cond, &g_tick_mutex, &deadline) == ETIMEDOUT) {
            break;
        }
    }
    pthread_mutex_unlock(&g_tick_mutex);
}

bool repl_is_paused(void)
{
    pthread_mutex_lock(&g_pause_mutex);
    bool p = g_paused;
    pthread_mutex_unlock(&g_pause_mutex);
    return p;
}

void repl_set_paused(bool paused)
{
    pthread_mutex_lock(&g_pause_mutex);
    g_paused = paused;
    if (!g_paused) {
        pthread_cond_broadcast(&g_pause_cond);
    }
    pthread_mutex_unlock(&g_pause_mutex);
}

void repl_wait_if_paused(void)
{
    pthread_mutex_lock(&g_pause_mutex);
    while (g_paused && server_is_running()) {
        pthread_cond_wait(&g_pause_cond, &g_pause_mutex);
    }
    pthread_mutex_unlock(&g_pause_mutex);
}

bool repl_is_leader(const char *topic, int partition)
{
    partition_info_t info;
    if (meta_get_partition(topic, partition, &info) != MB_OK) {
        return false;
    }
    return (info.leader == g_rank);
}

static part_state_t *get_or_create_part_state(const char *topic, int partition)
{
    pthread_mutex_lock(&g_part_states_mutex);
    part_state_t *curr = g_part_states;
    while (curr != NULL) {
        if (strcmp(curr->topic, topic) == 0 && curr->partition == partition) {
            pthread_mutex_unlock(&g_part_states_mutex);
            return curr;
        }
        curr = curr->next;
    }

    partition_info_t info;
    if (meta_get_partition(topic, partition, &info) != MB_OK) {
        pthread_mutex_unlock(&g_part_states_mutex);
        return NULL;
    }

    part_state_t *ps = calloc(1, sizeof(part_state_t));
    if (ps == NULL) {
        pthread_mutex_unlock(&g_part_states_mutex);
        return NULL;
    }

    snprintf(ps->topic, sizeof(ps->topic), "%s", topic);
    ps->partition = partition;
    ps->leader = info.leader;
    ps->n_replicas = info.n_replicas;
    pthread_mutex_init(&ps->mutex, NULL);
    pthread_cond_init(&ps->hwm_cond, NULL);

    double now = now_ms();
    for (int k = 0; k < info.n_replicas; k++) {
        ps->replicas[k] = info.replicas[k];
        ps->replica_states[k].rank = info.replicas[k];
        ps->replica_states[k].leo = 0;
        ps->replica_states[k].last_seen_ms = now;
        ps->replica_states[k].in_isr = true;
    }

    ps->next = g_part_states;
    g_part_states = ps;
    pthread_mutex_unlock(&g_part_states_mutex);
    return ps;
}

static void update_hwm_locked(part_state_t *ps, partition_log_t *log)
{
    int64_t min_leo = plog_end_offset(log);
    for (int k = 0; k < ps->n_replicas; k++) {
        if (ps->replicas[k] == ps->leader) {
            continue;
        }
        if (ps->replica_states[k].in_isr) {
            if (ps->replica_states[k].leo < min_leo) {
                min_leo = ps->replica_states[k].leo;
            }
        }
    }
    // A replica that hasn't sent a fetch request for a long time should be removed from the ISR
    // *before* calculating the minimum LEO, not just when heartbeats expire.
    // However, for now, this logic seems okay, assuming heartbeat_loop handles ISR removal.
    // The issue might be that HWM only moves up.
    if (min_leo > plog_hwm(log)) {
        plog_set_hwm(log, min_leo);
        pthread_cond_broadcast(&ps->hwm_cond);
    }
}

// State of a partition this broker keeps, as the leader sees it.
static int local_partition_state(const char *topic, int partition, int64_t *start,
                                 int64_t *hwm, int64_t *leo, int *isr, int *n_isr)
{
    partition_log_t *log = meta_get_log(topic, partition);
    if (log == NULL) {
        return MB_ERR_UNKNOWN_TOPIC;
    }

    if (start != NULL) {
        *start = plog_start_offset(log);
    }
    if (hwm != NULL) {
        *hwm = plog_hwm(log);
    }
    if (leo != NULL) {
        *leo = plog_end_offset(log);
    }

    part_state_t *ps = get_or_create_part_state(topic, partition);
    if (ps != NULL) {
        pthread_mutex_lock(&ps->mutex);
        int count = 0;
        for (int k = 0; k < ps->n_replicas; k++) {
            if (ps->replica_states[k].in_isr) {
                if (isr != NULL && count < META_MAX_REPLICAS) {
                    isr[count] = ps->replicas[k];
                }
                count++;
            }
        }
        if (n_isr != NULL) {
            *n_isr = count;
        }
        pthread_mutex_unlock(&ps->mutex);
    } else if (n_isr != NULL) {
        *n_isr = 0;
    }
    return MB_OK;
}

static int remote_partition_state(const char *topic, int partition, int leader,
                                  int64_t *start, int64_t *hwm, int64_t *leo,
                                  int *isr, int *n_isr)
{
    if (!peer_alive(leader)) {
        return MB_ERR_TIMEOUT;
    }

    state_req_t req;
    memset(&req, 0, sizeof req);
    snprintf(req.topic, sizeof req.topic, "%s", topic);
    req.partition = partition;

    void *reply = NULL;
    size_t reply_len = 0;
    int rc = mpil_request(leader, MPIL_TAG_STATE_REQ, &req, sizeof req, &reply, &reply_len,
                          STATE_TIMEOUT_MS);
    if (rc != MB_OK) {
        free(reply);
        return rc;
    }
    if (reply == NULL || reply_len < sizeof(state_resp_t)) {
        free(reply);
        return -1;
    }

    state_resp_t resp;
    memcpy(&resp, reply, sizeof resp);
    free(reply);
    if (resp.status != MB_OK) {
        return resp.status;
    }

    if (start != NULL) {
        *start = resp.start;
    }
    if (hwm != NULL) {
        *hwm = resp.hwm;
    }
    if (leo != NULL) {
        *leo = resp.leo;
    }
    if (isr != NULL && n_isr != NULL) {
        int count = resp.n_isr;
        if (count < 0) {
            count = 0;
        }
        if (count > META_MAX_REPLICAS) {
            count = META_MAX_REPLICAS;
        }
        for (int k = 0; k < count; k++) {
            isr[k] = resp.isr[k];
        }
        *n_isr = count;
    }
    return MB_OK;
}

// The leader is the only broker that knows the real ISR and HWM, so ask it
// whenever the partition is not led here. If it is paused, fall back to the
// local copy so `topic describe` still answers.
int repl_partition_state(const char *topic, int partition, int *isr, int *n_isr,
                         int64_t *hwm, int64_t *leo)
{
    partition_info_t info;
    if (meta_get_partition(topic, partition, &info) != MB_OK) {
        return MB_ERR_UNKNOWN_TOPIC;
    }

    if (info.leader == g_rank || g_size <= 1) {
        return local_partition_state(topic, partition, NULL, hwm, leo, isr, n_isr);
    }

    int rc = remote_partition_state(topic, partition, info.leader, NULL, hwm, leo, isr, n_isr);
    if (rc == MB_OK) {
        return MB_OK;
    }

    return local_partition_state(topic, partition, NULL, hwm, leo, isr, n_isr);
}

int repl_get_isr(const char *topic, int partition, int *isr, int *n_isr)
{
    return repl_partition_state(topic, partition, isr, n_isr, NULL, NULL);
}

int repl_partition_offsets(const char *topic, int partition, int64_t *start,
                           int64_t *hwm, int64_t *leo)
{
    partition_log_t *log = meta_get_log(topic, partition);
    if (log != NULL) {
        if (start != NULL) {
            *start = plog_start_offset(log);
        }
        if (hwm != NULL) {
            *hwm = plog_hwm(log);
        }
        if (leo != NULL) {
            *leo = plog_end_offset(log);
        }
        return MB_OK;
    }
    return repl_partition_state(topic, partition, NULL, NULL, hwm, leo);
}

int repl_wait_acks(const char *topic, int partition, int64_t offset, acks_t acks, int timeout_ms)
{
    if (acks == ACKS_NONE || acks == ACKS_LEADER) {
        return MB_OK;
    }

    partition_log_t *log = meta_get_log(topic, partition);
    if (log == NULL) {
        return MB_ERR_UNKNOWN_TOPIC;
    }

    topic_info_t tinfo;
    if (meta_topic_info(topic, &tinfo) != MB_OK) {
        return MB_ERR_UNKNOWN_TOPIC;
    }

    if (tinfo.replication <= 1) {
        plog_set_hwm(log, offset + 1);
        return MB_OK;
    }

    part_state_t *ps = get_or_create_part_state(topic, partition);
    if (ps == NULL) {
        return -1;
    }

    pthread_mutex_lock(&ps->mutex);
    // The leader just appended: recompute the HWM before sleeping so a produce
    // whose offset is already inside the ISR minimum does not wait a full tick.
    update_hwm_locked(ps, log);
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_ms / 1000;
    ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }

    int status = MB_OK;
    while (plog_hwm(log) <= offset && !atomic_load(&g_repl_stopping)) {
        int rc = pthread_cond_timedwait(&ps->hwm_cond, &ps->mutex, &ts);
        if (rc == ETIMEDOUT) {
            if (plog_hwm(log) <= offset) {
                status = MB_ERR_TIMEOUT;
            }
            break;
        }
    }

    if (atomic_load(&g_repl_stopping)) {
        status = MB_ERR_SHUTTING_DOWN;
    }
    pthread_mutex_unlock(&ps->mutex);
    return status;
}

static void handle_fetch_req(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    if (len < sizeof(uint64_t) + sizeof(fetch_req_t)) {
        return;
    }

    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));
    const fetch_req_t *req = (const fetch_req_t *)((const char *)buf + sizeof(uint64_t));

    partition_log_t *log = meta_get_log(req->topic, req->partition);
    if (log == NULL) {
        int64_t err_hwm = -1;
        mpil_reply(source, req_id, &err_hwm, sizeof(err_hwm));
        return;
    }

    part_state_t *ps = get_or_create_part_state(req->topic, req->partition);
    if (ps == NULL) {
        int64_t err_hwm = -1;
        mpil_reply(source, req_id, &err_hwm, sizeof(err_hwm));
        return;
    }

    pthread_mutex_lock(&ps->mutex);
    double now = now_ms();
    for (int k = 0; k < ps->n_replicas; k++) {
        if (ps->replicas[k] == req->replica_rank) {
            ps->replica_states[k].last_seen_ms = now;
            ps->replica_states[k].leo = req->from_offset;
            if (!ps->replica_states[k].in_isr && req->from_offset >= plog_hwm(log)) {
                ps->replica_states[k].in_isr = true;
            }
            break;
        }
    }
    update_hwm_locked(ps, log);
    int64_t current_hwm = plog_hwm(log);
    pthread_mutex_unlock(&ps->mutex);

    message_t *msgs = NULL;
    size_t count = 0;
    (void)plog_read(log, req->from_offset, FETCH_CHUNK_BYTES, false, &msgs, &count);

    size_t resp_len = sizeof(int64_t) + sizeof(uint32_t);
    for (size_t i = 0; i < count; i++) {
        resp_len += sizeof(int64_t) + sizeof(int64_t) + sizeof(uint32_t) + sizeof(uint32_t)
                    + msgs[i].key_len + msgs[i].value_len;
    }

    char *resp = malloc(resp_len);
    if (resp != NULL) {
        char *p = resp;
        memcpy(p, &current_hwm, sizeof(int64_t));
        p += sizeof(int64_t);
        uint32_t ucount = (uint32_t)count;
        memcpy(p, &ucount, sizeof(uint32_t));
        p += sizeof(uint32_t);

        for (size_t i = 0; i < count; i++) {
            memcpy(p, &msgs[i].offset, sizeof(int64_t)); p += sizeof(int64_t);
            memcpy(p, &msgs[i].timestamp_ms, sizeof(int64_t)); p += sizeof(int64_t);
            memcpy(p, &msgs[i].key_len, sizeof(uint32_t)); p += sizeof(uint32_t);
            memcpy(p, &msgs[i].value_len, sizeof(uint32_t)); p += sizeof(uint32_t);
            if (msgs[i].key_len > 0) {
                memcpy(p, msgs[i].key, msgs[i].key_len);
                p += msgs[i].key_len;
            }
            if (msgs[i].value_len > 0) {
                memcpy(p, msgs[i].value, msgs[i].value_len);
                p += msgs[i].value_len;
            }
        }

        mpil_reply(source, req_id, resp, resp_len);
        free(resp);
    }

    if (msgs != NULL) {
        plog_free_messages(msgs);
    }
    tick_advance();
}

static void handle_heartbeat(int source, const void *buf, size_t len, void *arg)
{
    (void)buf;
    (void)len;
    (void)arg;
    if (source >= 0 && source < 128) {
        pthread_mutex_lock(&g_peer_mutex);
        g_peer_last_seen_ms[source] = now_ms();
        pthread_mutex_unlock(&g_peer_mutex);
    }
    tick_advance();
}

static void handle_admin_msg(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    if (len < sizeof(uint64_t) + sizeof(admin_msg_t)) {
        return;
    }

    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));
    const admin_msg_t *msg = (const admin_msg_t *)((const char *)buf + sizeof(uint64_t));

    if (msg->cmd == ADMIN_PAUSE) {
        repl_set_paused(true);
    } else if (msg->cmd == ADMIN_RESUME) {
        repl_set_paused(false);
        // Catch up on metadata and replicas missed while paused.
        atomic_store(&g_need_metadata, true);
        tick_advance();
    } else if (msg->cmd == ADMIN_SHUTDOWN) {
        server_request_shutdown();
    }

    int ok = MB_OK;
    mpil_reply(source, req_id, &ok, sizeof(ok));
}

static void handle_meta_diffuse(int source, const void *buf, size_t len, void *arg)
{
    (void)source;
    (void)arg;
    if (len == 0 || buf == NULL) {
        return;
    }
    char *text = malloc(len + 1);
    if (text != NULL) {
        memcpy(text, buf, len);
        text[len] = '\0';
        meta_apply_snapshot(text);
        free(text);
    }
    tick_advance();
}

static void handle_meta_req(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    if (len < sizeof(uint64_t)) {
        return;
    }
    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));

    char *snapshot = NULL;
    size_t snapshot_len = 0;
    if (meta_get_snapshot(&snapshot, &snapshot_len) == 0 && snapshot != NULL) {
        mpil_reply(source, req_id, snapshot, snapshot_len);
        free(snapshot);
    } else {
        int err = -1;
        mpil_reply(source, req_id, &err, sizeof(err));
    }
}

static void handle_state_req(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    if (len < sizeof(uint64_t) + sizeof(state_req_t)) {
        return;
    }
    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));
    state_req_t req;
    memcpy(&req, (const char *)buf + sizeof(uint64_t), sizeof req);

    state_resp_t resp;
    memset(&resp, 0, sizeof resp);
    resp.start = -1;
    resp.hwm = -1;
    resp.leo = -1;
    resp.status = local_partition_state(req.topic, req.partition, &resp.start, &resp.hwm,
                                        &resp.leo, resp.isr, (int *)&resp.n_isr);
    mpil_reply(source, req_id, &resp, sizeof resp);
    tick_advance();
}

static void handle_read_req(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    if (len < sizeof(uint64_t) + sizeof(read_req_t)) {
        return;
    }
    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));
    read_req_t req;
    memcpy(&req, (const char *)buf + sizeof(uint64_t), sizeof req);
    if (req.max_bytes == 0 || req.max_bytes > MAX_REMOTE_READ_BYTES) {
        req.max_bytes = MAX_REMOTE_READ_BYTES;
    }

    read_resp_t resp;
    memset(&resp, 0, sizeof resp);
    resp.start = -1;
    resp.hwm = -1;
    resp.leo = -1;
    resp.status = MB_ERR_UNKNOWN_TOPIC;

    message_t *msgs = NULL;
    size_t count = 0;
    partition_log_t *log = meta_get_log(req.topic, req.partition);
    if (log != NULL) {
        resp.start = plog_start_offset(log);
        resp.hwm = plog_hwm(log);
        resp.leo = plog_end_offset(log);
        int rc = plog_read(log, req.from, (size_t)req.max_bytes, true, &msgs, &count);
        if (rc == MB_OK) {
            resp.status = MB_OK;
        } else if (rc == MB_ERR_OFFSET_OUT_OF_RANGE) {
            // The consumer is ahead of the high watermark: nothing to deliver yet.
            resp.status = MB_OK;
            count = 0;
        } else {
            resp.status = rc;
        }
    }

    if (resp.status == MB_OK) {
        resp.count = (uint32_t)count;
    }

    size_t resp_len = sizeof(resp);
    for (size_t i = 0; i < count; i++) {
        resp_len += sizeof(int64_t) + sizeof(int64_t) + sizeof(uint32_t) + sizeof(uint32_t)
                    + msgs[i].key_len + msgs[i].value_len;
    }

    char *out = malloc(resp_len);
    if (out != NULL) {
        char *p = out;
        memcpy(p, &resp, sizeof resp);
        p += sizeof resp;
        for (size_t i = 0; i < count; i++) {
            memcpy(p, &msgs[i].offset, sizeof(int64_t)); p += sizeof(int64_t);
            memcpy(p, &msgs[i].timestamp_ms, sizeof(int64_t)); p += sizeof(int64_t);
            memcpy(p, &msgs[i].key_len, sizeof(uint32_t)); p += sizeof(uint32_t);
            memcpy(p, &msgs[i].value_len, sizeof(uint32_t)); p += sizeof(uint32_t);
            if (msgs[i].key_len > 0) {
                memcpy(p, msgs[i].key, msgs[i].key_len);
                p += msgs[i].key_len;
            }
            if (msgs[i].value_len > 0) {
                memcpy(p, msgs[i].value, msgs[i].value_len);
                p += msgs[i].value_len;
            }
        }
        mpil_reply(source, req_id, out, resp_len);
        free(out);
    }

    if (msgs != NULL) {
        plog_free_messages(msgs);
    }
    tick_advance();
}

//----------------------------------------------------------------------------
// Reading a partition that may live on another broker
//----------------------------------------------------------------------------

int repl_read(const char *topic, int partition, int64_t from, size_t max_bytes,
              message_t **out, size_t *n)
{
    *out = NULL;
    *n = 0;

    partition_log_t *log = meta_get_log(topic, partition);
    if (log != NULL) {
        return plog_read(log, from, max_bytes, true, out, n);
    }

    partition_info_t info;
    if (meta_get_partition(topic, partition, &info) != MB_OK) {
        return MB_ERR_UNKNOWN_TOPIC;
    }
    if (info.leader == g_rank) {
        return MB_ERR_UNKNOWN_TOPIC;
    }
    if (!peer_alive(info.leader)) {
        return MB_ERR_TIMEOUT;
    }

    read_req_t req;
    memset(&req, 0, sizeof req);
    snprintf(req.topic, sizeof req.topic, "%s", topic);
    req.partition = partition;
    req.from = from;
    req.max_bytes = max_bytes;

    void *reply = NULL;
    size_t reply_len = 0;
    int rc = mpil_request(info.leader, MPIL_TAG_READ_REQ, &req, sizeof req, &reply, &reply_len,
                          READ_TIMEOUT_MS);
    if (rc != MB_OK) {
        free(reply);
        return rc;
    }
    if (reply == NULL || reply_len < sizeof(read_resp_t)) {
        free(reply);
        return -1;
    }

    read_resp_t resp;
    memcpy(&resp, reply, sizeof resp);
    if (resp.status != MB_OK) {
        free(reply);
        return resp.status;
    }

    size_t count = resp.count;
    const char *p = (const char *)reply + sizeof resp;
    const char *end = (const char *)reply + reply_len;
    size_t bytes = 0;
    for (size_t i = 0; i < count; i++) {
        if (p + sizeof(int64_t) * 2 + sizeof(uint32_t) * 2 > end) {
            free(reply);
            return -1;
        }
        p += sizeof(int64_t) * 2;
        uint32_t key_len = 0;
        uint32_t value_len = 0;
        memcpy(&key_len, p, sizeof(uint32_t));
        p += sizeof(uint32_t);
        memcpy(&value_len, p, sizeof(uint32_t));
        p += sizeof(uint32_t);
        if (p + key_len + value_len > end) {
            free(reply);
            return -1;
        }
        bytes += key_len + value_len;
        p += key_len + value_len;
    }

    message_t *msgs = malloc(count * sizeof(message_t) + bytes);
    if (msgs == NULL) {
        free(reply);
        return -1;
    }
    char *raw = (char *)(msgs + count);
    p = (const char *)reply + sizeof resp;
    size_t at = 0;
    for (size_t i = 0; i < count; i++) {
        int64_t offset = 0;
        int64_t timestamp = 0;
        uint32_t key_len = 0;
        uint32_t value_len = 0;
        memcpy(&offset, p, sizeof(int64_t)); p += sizeof(int64_t);
        memcpy(&timestamp, p, sizeof(int64_t)); p += sizeof(int64_t);
        memcpy(&key_len, p, sizeof(uint32_t)); p += sizeof(uint32_t);
        memcpy(&value_len, p, sizeof(uint32_t)); p += sizeof(uint32_t);

        msgs[i].offset = offset;
        msgs[i].timestamp_ms = timestamp;
        msgs[i].key_len = key_len;
        msgs[i].value_len = value_len;
        msgs[i].key = NULL;
        if (key_len > 0) {
            memcpy(raw + at, p, key_len);
            msgs[i].key = raw + at;
            at += key_len;
            p += key_len;
        }
        if (value_len > 0) {
            memcpy(raw + at, p, value_len);
            msgs[i].value = raw + at;
            at += value_len;
            p += value_len;
        } else {
            msgs[i].value = raw + at;
        }
    }

    free(reply);
    *out = msgs;
    *n = count;
    return MB_OK;
}

int repl_wait_data(const char *topic, int partition, int64_t from, int timeout_ms)
{
    partition_log_t *log = meta_get_log(topic, partition);
    if (log != NULL) {
        return plog_wait_data(log, from, true, timeout_ms);
    }

    double deadline = now_ms() + (double)timeout_ms;
    for (;;) {
        int64_t hwm = -1;
        int64_t leo = -1;
        uint64_t observed = tick_value();
        if (repl_partition_offsets(topic, partition, NULL, &hwm, &leo) == MB_OK && hwm > from) {
            return MB_OK;
        }
        if (atomic_load(&g_repl_stopping)) {
            return MB_ERR_SHUTTING_DOWN;
        }
        int remaining = (int)(deadline - now_ms());
        if (remaining <= 0) {
            return MB_ERR_TIMEOUT;
        }
        tick_wait(observed, remaining);
    }
}

//----------------------------------------------------------------------------
// Forwarded work: never block the MPI receiver thread
//----------------------------------------------------------------------------

static int forward_queue_push(int tag, int source, const void *buf, size_t len)
{
    if (len > 16u * 1024u * 1024u) {
        return -1;
    }
    forward_job_t *job = malloc(sizeof *job + len);
    if (job == NULL) {
        return -1;
    }
    job->tag = tag;
    job->source = source;
    job->len = len;
    job->next = NULL;
    if (len > 0) {
        memcpy(job->data, buf, len);
    }

    pthread_mutex_lock(&g_fwd_mutex);
    if (g_fwd_closed || g_fwd_count >= FORWARD_QUEUE_MAX) {
        pthread_mutex_unlock(&g_fwd_mutex);
        free(job);
        return -1;
    }
    if (g_fwd_tail == NULL) {
        g_fwd_head = job;
    } else {
        g_fwd_tail->next = job;
    }
    g_fwd_tail = job;
    g_fwd_count++;
    pthread_cond_signal(&g_fwd_cond);
    pthread_mutex_unlock(&g_fwd_mutex);
    return 0;
}

static forward_job_t *forward_queue_pop(void)
{
    pthread_mutex_lock(&g_fwd_mutex);
    while (g_fwd_head == NULL && !g_fwd_closed) {
        pthread_cond_wait(&g_fwd_cond, &g_fwd_mutex);
    }
    forward_job_t *job = g_fwd_head;
    if (job != NULL) {
        g_fwd_head = job->next;
        if (g_fwd_head == NULL) {
            g_fwd_tail = NULL;
        }
        g_fwd_count--;
    }
    pthread_mutex_unlock(&g_fwd_mutex);
    return job;
}

static void reply_queue_full(int tag, int source, const void *buf, size_t len)
{
    if (len < sizeof(uint64_t)) {
        return;
    }
    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));
    if (tag == MPIL_TAG_PRODUCE_REQ) {
        produce_resp_t resp = { .status = MB_ERR_SHUTTING_DOWN, .offset = -1 };
        mpil_reply(source, req_id, &resp, sizeof resp);
    } else {
        int err = MB_ERR_SHUTTING_DOWN;
        mpil_reply(source, req_id, &err, sizeof err);
    }
}

static void process_forward_req(int source, const void *buf, size_t len)
{
    if (len < sizeof(uint64_t) + 1) {
        return;
    }

    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));
    const char *payload = (const char *)buf + sizeof(uint64_t);
    size_t payload_len = len - sizeof(uint64_t);

    char line[LINE_MAX_LEN];
    if (payload_len >= sizeof(line)) {
        int err = -1;
        mpil_reply(source, req_id, &err, sizeof(err));
        return;
    }
    memcpy(line, payload, payload_len);
    line[payload_len] = '\0';

    if (strncmp(line, "CREATE ", 7) == 0) {
        char topic[META_NAME_MAX];
        int partitions = 0;
        int replication = 0;
        unsigned long long retention = 0;
        if (sscanf(line + 7, "%63s %d %d %llu", topic, &partitions, &replication, &retention) == 4) {
            int status = meta_create_topic(topic, partitions, replication, (uint64_t)retention);
            if (status == MB_OK) {
                repl_broadcast_metadata();
            }
            mpil_reply(source, req_id, &status, sizeof(status));
            return;
        }
    }

    groups_handle_forward(source, req_id, line, payload_len);
}

static void process_produce_req(int source, const void *buf, size_t len)
{
    if (len < sizeof(uint64_t) + sizeof(produce_req_t)) {
        return;
    }

    uint64_t req_id = 0;
    memcpy(&req_id, buf, sizeof(uint64_t));
    produce_req_t req;
    memcpy(&req, (const char *)buf + sizeof(uint64_t), sizeof req);

    const char *body = (const char *)buf + sizeof(uint64_t) + sizeof(produce_req_t);
    size_t body_len = len - sizeof(uint64_t) - sizeof(produce_req_t);
    if ((size_t)req.key_len + req.value_len > body_len) {
        produce_resp_t resp = { .status = MB_ERR_INVALID_ARGUMENT, .offset = -1 };
        if (req_id != 0) {
            mpil_reply(source, req_id, &resp, sizeof resp);
        }
        return;
    }

    produce_resp_t resp = { .status = MB_OK, .offset = -1 };
    partition_log_t *log = meta_get_log(req.topic, req.partition);
    if (log == NULL) {
        resp.status = MB_ERR_UNKNOWN_TOPIC;
    } else {
        message_t message;
        memset(&message, 0, sizeof message);
        message.key_len = req.key_len;
        message.value_len = req.value_len;
        message.key = (req.key_len > 0) ? (char *)body : NULL;
        message.value = (char *)(body + req.key_len);

        int64_t offset = -1;
        int status = plog_append(log, &message, 1, &offset);
        if (status != MB_OK) {
            resp.status = status;
        } else {
            topic_info_t tinfo;
            if (meta_topic_info(req.topic, &tinfo) == MB_OK && tinfo.replication <= 1) {
                plog_set_hwm(log, offset + 1);
            }
            if (req.acks == ACKS_ALL) {
                status = repl_wait_acks(req.topic, req.partition, offset, ACKS_ALL, PRODUCE_TIMEOUT_MS);
            }
            resp.status = status;
            resp.offset = offset;
        }
    }

    // req_id == 0 means the sender used acks=0 and does not wait for a reply.
    if (req_id != 0) {
        mpil_reply(source, req_id, &resp, sizeof resp);
    }
}

static void *forward_worker(void *arg)
{
    (void)arg;
    for (;;) {
        forward_job_t *job = forward_queue_pop();
        if (job == NULL) {
            break;
        }
        if (job->tag == MPIL_TAG_PRODUCE_REQ) {
            process_produce_req(job->source, job->data, job->len);
        } else {
            process_forward_req(job->source, job->data, job->len);
        }
        free(job);
    }
    return NULL;
}

static void handle_forward_req(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    if (forward_queue_push(MPIL_TAG_FORWARD_REQ, source, buf, len) != 0) {
        reply_queue_full(MPIL_TAG_FORWARD_REQ, source, buf, len);
    }
}

static void handle_produce_req(int source, const void *buf, size_t len, void *arg)
{
    (void)arg;
    if (forward_queue_push(MPIL_TAG_PRODUCE_REQ, source, buf, len) != 0) {
        reply_queue_full(MPIL_TAG_PRODUCE_REQ, source, buf, len);
    }
}

int repl_forward_produce(const char *topic, int partition, const char *key, size_t key_len,
                         const char *value, size_t value_len, acks_t acks, int64_t *offset)
{
    if (offset != NULL) {
        *offset = -1;
    }
    partition_info_t info;
    if (meta_get_partition(topic, partition, &info) != MB_OK) {
        return MB_ERR_UNKNOWN_TOPIC;
    }
    if (info.leader == g_rank) {
        return MB_ERR_NOT_LEADER;
    }
    if (!peer_alive(info.leader)) {
        // The leader is paused or down: the partition rejects writes until it
        // comes back (documented limitation, no leader election).
        return MB_ERR_TIMEOUT;
    }

    produce_req_t req;
    memset(&req, 0, sizeof req);
    snprintf(req.topic, sizeof req.topic, "%s", topic);
    req.partition = partition;
    req.acks = (int)acks;
    req.key_len = (uint32_t)key_len;
    req.value_len = (uint32_t)value_len;

    size_t total = sizeof(uint64_t) + sizeof(req) + key_len + value_len;
    char *buf = malloc(total);
    if (buf == NULL) {
        return -1;
    }
    uint64_t req_id = 0;
    memcpy(buf, &req_id, sizeof(uint64_t));
    memcpy(buf + sizeof(uint64_t), &req, sizeof req);
    char *body = buf + sizeof(uint64_t) + sizeof(req);
    if (key_len > 0 && key != NULL) {
        memcpy(body, key, key_len);
    }
    if (value_len > 0 && value != NULL) {
        memcpy(body + key_len, value, value_len);
    }

    if (acks == ACKS_NONE) {
        int rc = mpil_send(info.leader, MPIL_TAG_PRODUCE_REQ, buf, total);
        free(buf);
        return (rc == 0) ? MB_OK : MB_ERR_TIMEOUT;
    }

    void *reply = NULL;
    size_t reply_len = 0;
    int rc = mpil_request(info.leader, MPIL_TAG_PRODUCE_REQ, buf + sizeof(uint64_t),
                          total - sizeof(uint64_t), &reply, &reply_len, PRODUCE_TIMEOUT_MS);
    free(buf);
    if (rc != MB_OK) {
        free(reply);
        return rc;
    }
    if (reply == NULL || reply_len < sizeof(produce_resp_t)) {
        free(reply);
        return -1;
    }
    produce_resp_t resp;
    memcpy(&resp, reply, sizeof resp);
    free(reply);
    if (resp.status == MB_OK && offset != NULL) {
        *offset = resp.offset;
    }
    return resp.status;
}

//----------------------------------------------------------------------------
// Metadata diffusion
//----------------------------------------------------------------------------

void repl_broadcast_metadata(void)
{
    char *snapshot = NULL;
    size_t len = 0;
    if (meta_get_snapshot(&snapshot, &len) == 0 && snapshot != NULL) {
        for (int r = 0; r < g_size; r++) {
            if (r != g_rank) {
                mpil_send(r, MPIL_TAG_META_DIFFUSE, snapshot, len);
            }
        }
        free(snapshot);
    }
}

int repl_request_metadata(void)
{
    if (g_rank == 0) {
        return 0;
    }
    void *reply = NULL;
    size_t reply_len = 0;
    int rc = mpil_request(0, MPIL_TAG_META_REQ, NULL, 0, &reply, &reply_len, 3000);
    if (rc == MB_OK && reply != NULL && reply_len > 0) {
        char *text = malloc(reply_len + 1);
        if (text != NULL) {
            memcpy(text, reply, reply_len);
            text[reply_len] = '\0';
            meta_apply_snapshot(text);
            free(text);
        }
    }
    free(reply);
    return rc;
}

int repl_forward_topic_create(const char *name, int partitions, int replication, uint64_t retention_bytes)
{
    char req[LINE_MAX_LEN];
    snprintf(req, sizeof(req), "CREATE %s %d %d %llu", name, partitions, replication,
             (unsigned long long)retention_bytes);

    void *reply = NULL;
    size_t reply_len = 0;
    int rc = mpil_request(0, MPIL_TAG_FORWARD_REQ, req, strlen(req) + 1, &reply, &reply_len, 5000);
    if (rc != MB_OK) {
        free(reply);
        return rc;
    }

    int status = -1;
    if (reply != NULL && reply_len >= sizeof(int)) {
        memcpy(&status, reply, sizeof(int));
    }
    free(reply);
    return status;
}

//----------------------------------------------------------------------------
// Background threads
//----------------------------------------------------------------------------

static void *fetch_loop(void *arg)
{
    (void)arg;
    while (!atomic_load(&g_repl_stopping)) {
        if (repl_is_paused()) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 100000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            pthread_mutex_lock(&g_fetch_mutex);
            pthread_cond_timedwait(&g_fetch_cond, &g_fetch_mutex, &ts);
            pthread_mutex_unlock(&g_fetch_mutex);
            continue;
        }

        // A resumed broker may have missed topic creations while paused.
        if (atomic_exchange(&g_need_metadata, false) && g_rank != 0 && peer_alive(0)) {
            (void)repl_request_metadata();
        }

        char list_buf[4096];
        if (meta_list(list_buf, sizeof(list_buf)) == MB_OK) {
            char *line = list_buf;
            while (*line != '\0' && !atomic_load(&g_repl_stopping)) {
                char *next = strchr(line, '\n');
                if (next != NULL) {
                    *next = '\0';
                }
                char topic[META_NAME_MAX];
                int partitions = 0;
                int replication = 0;
                unsigned long long retention = 0;
                if (sscanf(line, "%63s partitions=%d replication=%d retention.bytes=%llu",
                           topic, &partitions, &replication, &retention) >= 3) {
                    for (int p = 0; p < partitions && !atomic_load(&g_repl_stopping); p++) {
                        partition_info_t pinfo;
                        if (meta_get_partition(topic, p, &pinfo) != MB_OK) {
                            continue;
                        }
                        if (pinfo.leader == g_rank) {
                            continue;
                        }

                        bool am_replica = false;
                        for (int k = 0; k < pinfo.n_replicas; k++) {
                            if (pinfo.replicas[k] == g_rank) {
                                am_replica = true;
                                break;
                            }
                        }
                        if (!am_replica) {
                            continue;
                        }

                        partition_log_t *log = meta_get_log(topic, p);
                        if (log == NULL) {
                            continue;
                        }

                        int64_t from_offset = plog_end_offset(log);
                        fetch_req_t req;
                        memset(&req, 0, sizeof req);
                        snprintf(req.topic, sizeof req.topic, "%s", topic);
                        req.partition = p;
                        req.from_offset = from_offset;
                        req.replica_rank = g_rank;

                        void *reply = NULL;
                        size_t reply_len = 0;
                        int rc = mpil_request(pinfo.leader, MPIL_TAG_FETCH_REQ, &req, sizeof(req),
                                             &reply, &reply_len, 1000);
                        if (rc == MB_OK && reply != NULL
                            && reply_len >= sizeof(int64_t) + sizeof(uint32_t)) {
                            const char *ptr = (const char *)reply;
                            int64_t leader_hwm = 0;
                            memcpy(&leader_hwm, ptr, sizeof(int64_t));
                            ptr += sizeof(int64_t);
                            uint32_t count = 0;
                            memcpy(&count, ptr, sizeof(uint32_t));
                            ptr += sizeof(uint32_t);

                            if (leader_hwm >= 0 && count > 0) {
                                message_t *msgs = calloc(count, sizeof(message_t));
                                bool parse_ok = (msgs != NULL);
                                size_t consumed = sizeof(int64_t) + sizeof(uint32_t);

                                for (uint32_t i = 0; parse_ok && i < count; i++) {
                                    if (consumed + sizeof(int64_t) * 2 + sizeof(uint32_t) * 2 > reply_len) {
                                        parse_ok = false;
                                        break;
                                    }
                                    memcpy(&msgs[i].offset, ptr, sizeof(int64_t));
                                    ptr += sizeof(int64_t);
                                    memcpy(&msgs[i].timestamp_ms, ptr, sizeof(int64_t));
                                    ptr += sizeof(int64_t);
                                    memcpy(&msgs[i].key_len, ptr, sizeof(uint32_t));
                                    ptr += sizeof(uint32_t);
                                    memcpy(&msgs[i].value_len, ptr, sizeof(uint32_t));
                                    ptr += sizeof(uint32_t);
                                    consumed += sizeof(int64_t) * 2 + sizeof(uint32_t) * 2;

                                    if (consumed + msgs[i].key_len + msgs[i].value_len > reply_len) {
                                        parse_ok = false;
                                        break;
                                    }
                                    if (msgs[i].key_len > 0) {
                                        msgs[i].key = malloc(msgs[i].key_len);
                                        if (msgs[i].key == NULL) {
                                            parse_ok = false;
                                            break;
                                        }
                                        memcpy(msgs[i].key, ptr, msgs[i].key_len);
                                        ptr += msgs[i].key_len;
                                    }
                                    if (msgs[i].value_len > 0) {
                                        msgs[i].value = malloc(msgs[i].value_len);
                                        if (msgs[i].value == NULL) {
                                            parse_ok = false;
                                            break;
                                        }
                                        memcpy(msgs[i].value, ptr, msgs[i].value_len);
                                        ptr += msgs[i].value_len;
                                    }
                                    consumed += msgs[i].key_len + msgs[i].value_len;
                                }

                                if (parse_ok) {
                                    plog_append_replica(log, msgs, count);
                                }

                                for (uint32_t i = 0; i < count; i++) {
                                    free(msgs[i].key);
                                    free(msgs[i].value);
                                }
                                free(msgs);
                            }

                            if (leader_hwm >= 0) {
                                plog_set_hwm(log, leader_hwm);
                            }
                            tick_advance();
                        }
                        free(reply);
                    }
                }
                if (next == NULL) {
                    break;
                }
                line = next + 1;
            }
        }

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += (long)FETCH_IDLE_MS * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_mutex_lock(&g_fetch_mutex);
        pthread_cond_timedwait(&g_fetch_cond, &g_fetch_mutex, &ts);
        pthread_mutex_unlock(&g_fetch_mutex);
    }
    return NULL;
}

static void *heartbeat_loop(void *arg)
{
    (void)arg;
    while (!atomic_load(&g_repl_stopping)) {
        if (!repl_is_paused()) {
            for (int r = 0; r < g_size; r++) {
                if (r != g_rank) {
                    mpil_send(r, MPIL_TAG_HEARTBEAT, &g_rank, sizeof(int));
                }
            }

            pthread_mutex_lock(&g_part_states_mutex);
            part_state_t *ps = g_part_states;
            double now = now_ms();
            while (ps != NULL) {
                if (ps->leader == g_rank) {
                    pthread_mutex_lock(&ps->mutex);
                    for (int k = 0; k < ps->n_replicas; k++) {
                        if (ps->replicas[k] == g_rank) {
                            continue;
                        }
                        if (ps->replica_states[k].in_isr
                            && now - ps->replica_states[k].last_seen_ms > REPLICA_TIMEOUT_MS) {
                            ps->replica_states[k].in_isr = false;
                        }
                    }
                    // Recompute every tick: once a replica leaves the ISR, the
                    // HWM can advance to the leader's LEO even with no fetch.
                    partition_log_t *log = meta_get_log(ps->topic, ps->partition);
                    if (log != NULL) {
                        update_hwm_locked(ps, log);
                    }
                    pthread_mutex_unlock(&ps->mutex);
                }
                ps = ps->next;
            }
            pthread_mutex_unlock(&g_part_states_mutex);
        }

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += (long)HEARTBEAT_INTERVAL_MS * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += ts.tv_nsec / 1000000000L;
            ts.tv_nsec %= 1000000000L;
        }
        pthread_mutex_lock(&g_pause_mutex);
        pthread_cond_timedwait(&g_pause_cond, &g_pause_mutex, &ts);
        pthread_mutex_unlock(&g_pause_mutex);
    }
    return NULL;
}

//----------------------------------------------------------------------------
// Lifecycle
//----------------------------------------------------------------------------

int repl_init(int rank, int size)
{
    g_rank = rank;
    g_size = size;
    g_repl_started = false;
    atomic_store(&g_repl_stopping, false);
    g_fwd_closed = false;
    g_fwd_head = NULL;
    g_fwd_tail = NULL;
    g_fwd_count = 0;
    g_fwd_started = false;

    double now = now_ms();
    for (int i = 0; i < 128; i++) {
        g_peer_last_seen_ms[i] = now;
    }

    mpil_register(MPIL_TAG_FETCH_REQ, handle_fetch_req, NULL);
    mpil_register(MPIL_TAG_HEARTBEAT, handle_heartbeat, NULL);
    mpil_register(MPIL_TAG_ADMIN, handle_admin_msg, NULL);
    mpil_register(MPIL_TAG_META_DIFFUSE, handle_meta_diffuse, NULL);
    mpil_register(MPIL_TAG_META_REQ, handle_meta_req, NULL);
    mpil_register(MPIL_TAG_STATE_REQ, handle_state_req, NULL);
    mpil_register(MPIL_TAG_READ_REQ, handle_read_req, NULL);
    mpil_register(MPIL_TAG_FORWARD_REQ, handle_forward_req, NULL);
    mpil_register(MPIL_TAG_PRODUCE_REQ, handle_produce_req, NULL);

    return 0;
}

int repl_start(void)
{
    if (g_repl_started) {
        return 0;
    }
    g_repl_started = true;
    atomic_store(&g_repl_stopping, false);

    // Every broker must have its handlers registered and its metadata loaded
    // before the controller diffuses anything.
    mpil_barrier();

    for (int i = 0; i < FORWARD_WORKERS; i++) {
        if (pthread_create(&g_fwd_threads[i], NULL, forward_worker, NULL) != 0) {
            perror("pthread_create forward_worker");
            g_fwd_threads[i] = 0;
            break;
        }
    }
    g_fwd_started = true;

    if (pthread_create(&g_fetch_thread, NULL, fetch_loop, NULL) != 0) {
        perror("pthread_create fetch_loop");
        return -1;
    }

    if (pthread_create(&g_heartbeat_thread, NULL, heartbeat_loop, NULL) != 0) {
        perror("pthread_create heartbeat_loop");
        return -1;
    }

    if (g_rank == 0) {
        repl_broadcast_metadata();
    } else {
        (void)repl_request_metadata();
    }

    return 0;
}

void repl_shutdown(void)
{
    if (!g_repl_started || atomic_load(&g_repl_stopping)) {
        return;
    }
    atomic_store(&g_repl_stopping, true);

    // Wake anything waiting for a pause, for data or for a forwarded reply.
    repl_set_paused(false);

    pthread_mutex_lock(&g_fetch_mutex);
    pthread_cond_broadcast(&g_fetch_cond);
    pthread_mutex_unlock(&g_fetch_mutex);

    pthread_mutex_lock(&g_tick_mutex);
    pthread_cond_broadcast(&g_tick_cond);
    pthread_mutex_unlock(&g_tick_mutex);

    pthread_mutex_lock(&g_part_states_mutex);
    part_state_t *ps = g_part_states;
    while (ps != NULL) {
        pthread_mutex_lock(&ps->mutex);
        pthread_cond_broadcast(&ps->hwm_cond);
        pthread_mutex_unlock(&ps->mutex);
        ps = ps->next;
    }
    pthread_mutex_unlock(&g_part_states_mutex);

    mpil_abort_requests();

    pthread_join(g_fetch_thread, NULL);
    pthread_join(g_heartbeat_thread, NULL);

    // Drain the forwarded requests before the collective barrier.
    pthread_mutex_lock(&g_fwd_mutex);
    g_fwd_closed = true;
    pthread_cond_broadcast(&g_fwd_cond);
    pthread_mutex_unlock(&g_fwd_mutex);

    if (g_fwd_started) {
        for (int i = 0; i < FORWARD_WORKERS; i++) {
            if (g_fwd_threads[i] != 0) {
                pthread_join(g_fwd_threads[i], NULL);
                g_fwd_threads[i] = 0;
            }
        }
        g_fwd_started = false;
    }

    // Distributed barrier: nobody finalizes MPI while another broker still
    // has something to send.
    mpil_barrier();
    mpil_shutdown();

    g_repl_started = false;
}

int cluster_admin(int broker_id, admin_cmd_t command)
{
    if (command == ADMIN_SHUTDOWN) {
        if (broker_id < 0) {
            for (int r = 0; r < g_size; r++) {
                if (r != g_rank) {
                    admin_msg_t msg = { .cmd = ADMIN_SHUTDOWN };
                    void *reply = NULL;
                    size_t reply_len = 0;
                    (void)mpil_request(r, MPIL_TAG_ADMIN, &msg, sizeof(msg), &reply, &reply_len, 1000);
                    free(reply);
                }
            }
        } else if (broker_id != g_rank) {
            admin_msg_t msg = { .cmd = ADMIN_SHUTDOWN };
            void *reply = NULL;
            size_t reply_len = 0;
            (void)mpil_request(broker_id, MPIL_TAG_ADMIN, &msg, sizeof(msg), &reply, &reply_len, 1000);
            free(reply);
        }
        server_request_shutdown();
        return MB_OK;
    }

    if (broker_id == g_rank) {
        if (command == ADMIN_PAUSE) {
            repl_set_paused(true);
        } else if (command == ADMIN_RESUME) {
            repl_set_paused(false);
            atomic_store(&g_need_metadata, true);
            tick_advance();
        }
        return MB_OK;
    }

    if (broker_id < 0 || broker_id >= g_size) {
        return MB_ERR_INVALID_ARGUMENT;
    }

    admin_msg_t msg = { .cmd = command };
    void *reply = NULL;
    size_t reply_len = 0;
    int rc = mpil_request(broker_id, MPIL_TAG_ADMIN, &msg, sizeof(msg), &reply, &reply_len, 5000);
    free(reply);
    return rc;
}
