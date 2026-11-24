#define _POSIX_C_SOURCE 200809L

#include "metadata.h"
#include "common.h"
#include "fsutil.h"
#include "log.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

__attribute__((weak)) int repl_partition_state(const char *topic, int partition, int *isr, int *n_isr, int64_t *hwm, int64_t *leo);

#define META_FILE "metadata"
#define LINE_MAX_BYTES 256

typedef struct {
    char name[META_NAME_MAX];
    int partitions;
    int replication;
    uint64_t retention_bytes;
    partition_info_t *parts;
    partition_log_t **logs; // logs[p] is NULL when this broker does not keep partition p
} topic_t;

typedef struct {
    uint64_t version;
    topic_t *topics;
    size_t count;
    size_t cap;
} table_t;

static struct {
    pthread_mutex_t mutex;
    bool ready;
    int rank;
    int size;
    char path[PATH_MAX];
    char dir[PATH_MAX];
    table_t table;
} g_meta = { .mutex = PTHREAD_MUTEX_INITIALIZER };

static void close_logs(topic_t *t)
{
    if (t->logs == NULL) {
        return;
    }
    for (int p = 0; p < t->partitions; p++) {
        plog_close(t->logs[p]);
    }
    free(t->logs);
    t->logs = NULL;
}

static bool keeps_partition(const partition_info_t *info, int rank)
{
    for (int k = 0; k < info->n_replicas; k++) {
        if (info->replicas[k] == rank) {
            return true;
        }
    }
    return false;
}

// Partition p of topic t lives in <dir>/topics/<t>/<p>.
static int open_logs(topic_t *t, const char *dir, int rank)
{
    t->logs = calloc((size_t)t->partitions, sizeof *t->logs);
    if (t->logs == NULL) {
        return -1;
    }
    for (int p = 0; p < t->partitions; p++) {
        if (!keeps_partition(&t->parts[p], rank)) {
            continue;
        }
        char path[PATH_MAX + META_NAME_MAX + 32];
        int len = snprintf(path, sizeof path, "%s/topics/%s/%d", dir, t->name, p);
        if (len < 0 || (size_t)len >= sizeof path) {
            close_logs(t);
            return -1;
        }
        t->logs[p] = plog_open(path, t->retention_bytes);
        if (t->logs[p] == NULL) {
            close_logs(t);
            return -1;
        }
    }
    return 0;
}

static void table_free(table_t *t)
{
    for (size_t i = 0; i < t->count; i++) {
        close_logs(&t->topics[i]);
        free(t->topics[i].parts);
    }
    free(t->topics);
    t->topics = NULL;
    t->count = 0;
    t->cap = 0;
    t->version = 0;
}

static topic_t *table_find(const table_t *t, const char *name)
{
    for (size_t i = 0; i < t->count; i++) {
        if (strcmp(t->topics[i].name, name) == 0) {
            return &t->topics[i];
        }
    }
    return NULL;
}

static int table_push(table_t *t, const topic_t *topic)
{
    if (t->count == t->cap) {
        size_t cap = 8;
        if (t->cap > 0) {
            cap = t->cap * 2;
        }
        topic_t *grown = realloc(t->topics, cap * sizeof *grown);
        if (grown == NULL) {
            return -1;
        }
        t->topics = grown;
        t->cap = cap;
    }
    t->topics[t->count] = *topic;
    t->count++;
    return 0;
}

static bool name_valid(const char *name)
{
    if (name == NULL) {
        return false;
    }
    size_t len = strlen(name);
    if (len == 0 || len >= META_NAME_MAX || strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)name[i];
        if (!isalnum(c) && c != '.' && c != '_' && c != '-') {
            return false;
        }
    }
    return true;
}

static void write_replicas(FILE *f, const partition_info_t *info)
{
    for (int k = 0; k < info->n_replicas; k++) {
        if (k > 0) {
            fputc(',', f);
        }
        fprintf(f, "%d", info->replicas[k]);
    }
}

// Snapshot format:
//   version <n>
//   topic <name> <partitions> <replication> <retention_bytes>
//   <partition> <leader> <replica>,<replica>,...   (one line per partition)
static int snapshot_build(const table_t *t, char **out, size_t *len)
{
    FILE *f = open_memstream(out, len);
    if (f == NULL) {
        return -1;
    }
    fprintf(f, "version %llu\n", (unsigned long long)t->version);
    for (size_t i = 0; i < t->count; i++) {
        const topic_t *topic = &t->topics[i];
        fprintf(f, "topic %s %d %d %llu\n", topic->name, topic->partitions, topic->replication,
                (unsigned long long)topic->retention_bytes);
        for (int p = 0; p < topic->partitions; p++) {
            fprintf(f, "%d %d ", p, topic->parts[p].leader);
            write_replicas(f, &topic->parts[p]);
            fputc('\n', f);
        }
    }
    if (fclose(f) != 0) {
        free(*out);
        return -1;
    }
    return 0;
}

static int parse_topic_line(const char *line, int brokers, table_t *t)
{
    char name[META_NAME_MAX];
    int partitions = 0;
    int replication = 0;
    unsigned long long retention = 0;
    int used = 0;
    if (sscanf(line, "topic %63s %d %d %llu%n", name, &partitions, &replication, &retention,
               &used) != 4 || line[used] != '\0') {
        return -1;
    }
    if (!name_valid(name) || table_find(t, name) != NULL || partitions < 1
        || partitions > META_MAX_PARTITIONS || replication < 1 || replication > brokers
        || replication > META_MAX_REPLICAS) {
        return -1;
    }
    topic_t topic;
    memset(&topic, 0, sizeof topic);
    snprintf(topic.name, sizeof topic.name, "%s", name);
    topic.partitions = partitions;
    topic.replication = replication;
    topic.retention_bytes = (uint64_t)retention;
    topic.parts = calloc((size_t)partitions, sizeof *topic.parts);
    if (topic.parts == NULL || table_push(t, &topic) != 0) {
        free(topic.parts);
        return -1;
    }
    return 0;
}

static int parse_partition_line(const char *line, int brokers, topic_t *topic, int index)
{
    int p = -1;
    int leader = -1;
    int used = 0;
    if (sscanf(line, "%d %d %n", &p, &leader, &used) != 2 || used == 0 || p != index) {
        return -1;
    }
    partition_info_t *info = &topic->parts[index];
    const char *s = line + used;
    bool leader_found = false;
    info->n_replicas = 0;
    while (*s != '\0') {
        char *end = NULL;
        errno = 0;
        long v = strtol(s, &end, 10);
        if (end == s || errno != 0 || v < 0 || v >= brokers || info->n_replicas >= META_MAX_REPLICAS) {
            return -1;
        }
        for (int k = 0; k < info->n_replicas; k++) {
            if (info->replicas[k] == (int)v) {
                return -1;
            }
        }
        if (v == leader) {
            leader_found = true;
        }
        info->replicas[info->n_replicas] = (int)v;
        info->n_replicas++;
        s = end;
        if (*s == ',') {
            s++;
        } else if (*s != '\0') {
            return -1;
        }
    }
    if (!leader_found || info->n_replicas != topic->replication) {
        return -1;
    }
    info->leader = leader;
    return 0;
}

static int snapshot_parse(const char *text, int brokers, table_t *out)
{
    table_t t;
    memset(&t, 0, sizeof t);
    bool have_version = false;
    int next_part = 0;
    const char *line = text;
    while (*line != '\0') {
        const char *end = strchr(line, '\n');
        size_t len = strlen(line);
        if (end != NULL) {
            len = (size_t)(end - line);
        }
        char buf[LINE_MAX_BYTES];
        if (len >= sizeof buf) {
            goto fail;
        }
        memcpy(buf, line, len);
        buf[len] = '\0';
        line += len;
        if (*line == '\n') {
            line++;
        }
        if (len == 0) {
            continue;
        }
        topic_t *current = NULL;
        if (t.count > 0) {
            current = &t.topics[t.count - 1];
        }
        if (!have_version) {
            unsigned long long version = 0;
            int used = 0;
            if (sscanf(buf, "version %llu%n", &version, &used) != 1 || buf[used] != '\0') {
                goto fail;
            }
            t.version = (uint64_t)version;
            have_version = true;
        } else if (strncmp(buf, "topic ", 6) == 0) {
            if ((current != NULL && next_part != current->partitions)
                || parse_topic_line(buf, brokers, &t) != 0) {
                goto fail;
            }
            next_part = 0;
        } else {
            if (current == NULL || next_part >= current->partitions
                || parse_partition_line(buf, brokers, current, next_part) != 0) {
                goto fail;
            }
            next_part++;
        }
    }
    if (!have_version || (t.count > 0 && next_part != t.topics[t.count - 1].partitions)) {
        goto fail;
    }
    *out = t;
    return 0;
fail:
    table_free(&t);
    return -1;
}

static int table_save(const table_t *t)
{
    char *text = NULL;
    size_t len = 0;
    if (snapshot_build(t, &text, &len) != 0) {
        return -1;
    }
    int rc = fs_write_atomic(g_meta.path, text, len);
    free(text);
    return rc;
}

int meta_init(int rank, int size, const char *data_dir)
{
    if (data_dir == NULL || size < 1 || rank < 0 || rank >= size) {
        return -1;
    }
    pthread_mutex_lock(&g_meta.mutex);
    int rc = -1;
    if (g_meta.ready || fs_mkdir_p(data_dir) != 0) {
        goto out;
    }
    int len = snprintf(g_meta.path, sizeof g_meta.path, "%s/%s", data_dir, META_FILE);
    if (len < 0 || (size_t)len >= sizeof g_meta.path) {
        goto out;
    }
    table_t t;
    memset(&t, 0, sizeof t);
    char *text = NULL;
    size_t text_len = 0;
    if (fs_read_file(g_meta.path, &text, &text_len) == 0) {
        int parsed = -1;
        if (strlen(text) == text_len) {
            parsed = snapshot_parse(text, size, &t);
        }
        free(text);
        if (parsed != 0) {
            errno = EIO;
            goto out;
        }
    } else if (errno != ENOENT) {
        goto out;
    }
    for (size_t i = 0; i < t.count; i++) {
        if (open_logs(&t.topics[i], data_dir, rank) != 0) {
            table_free(&t);
            goto out;
        }
    }
    snprintf(g_meta.dir, sizeof g_meta.dir, "%s", data_dir);
    g_meta.table = t;
    g_meta.rank = rank;
    g_meta.size = size;
    g_meta.ready = true;
    rc = 0;
out:
    pthread_mutex_unlock(&g_meta.mutex);
    return rc;
}

void meta_destroy(void)
{
    pthread_mutex_lock(&g_meta.mutex);
    table_free(&g_meta.table);
    g_meta.ready = false;
    pthread_mutex_unlock(&g_meta.mutex);
}

static int create_locked(const char *name, int partitions, int replication, uint64_t retention_bytes)
{
    topic_t topic;
    memset(&topic, 0, sizeof topic);
    snprintf(topic.name, sizeof topic.name, "%s", name);
    topic.partitions = partitions;
    topic.replication = replication;
    topic.retention_bytes = retention_bytes;
    topic.parts = calloc((size_t)partitions, sizeof *topic.parts);
    if (topic.parts == NULL) {
        return -1;
    }
    int first = (int)(g_meta.table.count % (size_t)g_meta.size);
    for (int p = 0; p < partitions; p++) {
        partition_info_t *info = &topic.parts[p];
        info->leader = (first + p) % g_meta.size;
        info->n_replicas = replication;
        for (int k = 0; k < replication; k++) {
            info->replicas[k] = (info->leader + k) % g_meta.size;
        }
    }
    if (open_logs(&topic, g_meta.dir, g_meta.rank) != 0) {
        free(topic.parts);
        return -1;
    }
    if (table_push(&g_meta.table, &topic) != 0) {
        close_logs(&topic);
        free(topic.parts);
        return -1;
    }
    g_meta.table.version++;
    if (table_save(&g_meta.table) != 0) {
        g_meta.table.version--;
        g_meta.table.count--;
        close_logs(&topic);
        free(topic.parts);
        return -1;
    }
    return MB_OK;
}

int meta_create_topic(const char *name, int partitions, int replication, uint64_t retention_bytes)
{
    if (!name_valid(name) || partitions < 1 || partitions > META_MAX_PARTITIONS || replication < 1
        || replication > META_MAX_REPLICAS) {
        return MB_ERR_INVALID_ARGUMENT;
    }
    pthread_mutex_lock(&g_meta.mutex);
    int rc = -1;
    if (!g_meta.ready) {
        rc = -1;
    } else if (g_meta.rank != 0) {
        rc = MB_ERR_NOT_LEADER;
    } else if (replication > g_meta.size) {
        rc = MB_ERR_INVALID_ARGUMENT;
    } else if (table_find(&g_meta.table, name) != NULL) {
        rc = MB_ERR_TOPIC_EXISTS;
    } else {
        rc = create_locked(name, partitions, replication, retention_bytes);
    }
    pthread_mutex_unlock(&g_meta.mutex);
    return rc;
}

int meta_topic_info(const char *topic, topic_info_t *out)
{
    if (topic == NULL || out == NULL) {
        return -1;
    }
    pthread_mutex_lock(&g_meta.mutex);
    int rc = -1;
    const topic_t *t = NULL;
    if (g_meta.ready) {
        t = table_find(&g_meta.table, topic);
        rc = MB_ERR_UNKNOWN_TOPIC;
    }
    if (t != NULL) {
        out->partitions = t->partitions;
        out->replication = t->replication;
        out->retention_bytes = t->retention_bytes;
        rc = MB_OK;
    }
    pthread_mutex_unlock(&g_meta.mutex);
    return rc;
}

int meta_get_partition(const char *topic, int partition, partition_info_t *out)
{
    if (topic == NULL || out == NULL) {
        return -1;
    }
    pthread_mutex_lock(&g_meta.mutex);
    int rc = -1;
    const topic_t *t = NULL;
    if (g_meta.ready) {
        t = table_find(&g_meta.table, topic);
        rc = MB_ERR_UNKNOWN_TOPIC;
    }
    if (t != NULL) {
        rc = MB_ERR_INVALID_ARGUMENT;
        if (partition >= 0 && partition < t->partitions) {
            *out = t->parts[partition];
            rc = MB_OK;
        }
    }
    pthread_mutex_unlock(&g_meta.mutex);
    return rc;
}

partition_log_t *meta_get_log(const char *topic, int partition)
{
    if (topic == NULL) {
        return NULL;
    }
    pthread_mutex_lock(&g_meta.mutex);
    partition_log_t *log = NULL;
    const topic_t *t = NULL;
    if (g_meta.ready) {
        t = table_find(&g_meta.table, topic);
    }
    if (t != NULL && t->logs != NULL && partition >= 0 && partition < t->partitions) {
        log = t->logs[partition];
    }
    pthread_mutex_unlock(&g_meta.mutex);
    return log;
}

int meta_list(char *out, size_t size)
{
    if (out == NULL || size == 0) {
        return -1;
    }
    char *text = NULL;
    size_t len = 0;
    pthread_mutex_lock(&g_meta.mutex);
    int rc = -1;
    FILE *f = NULL;
    if (g_meta.ready) {
        f = open_memstream(&text, &len);
    }
    if (f != NULL) {
        for (size_t i = 0; i < g_meta.table.count; i++) {
            const topic_t *t = &g_meta.table.topics[i];
            fprintf(f, "%s partitions=%d replication=%d retention.bytes=%llu\n", t->name,
                    t->partitions, t->replication, (unsigned long long)t->retention_bytes);
        }
        if (fclose(f) == 0) {
            rc = MB_OK;
        }
    }
    pthread_mutex_unlock(&g_meta.mutex);
    if (rc == MB_OK) {
        snprintf(out, size, "%s", text);
    }
    free(text);
    return rc;
}

int meta_describe(const char *topic, char *out, size_t size)
{
    if (topic == NULL || out == NULL || size == 0) {
        return -1;
    }

    // Copy what we need and release the metadata mutex before asking the
    // replication module: it calls back into meta_get_partition/meta_get_log.
    char name[META_NAME_MAX];
    int partitions = 0;
    int replication = 0;
    uint64_t retention_bytes = 0;
    partition_info_t *parts = NULL;

    pthread_mutex_lock(&g_meta.mutex);
    const topic_t *t = NULL;
    if (g_meta.ready) {
        t = table_find(&g_meta.table, topic);
    }
    if (t == NULL) {
        pthread_mutex_unlock(&g_meta.mutex);
        return g_meta.ready ? MB_ERR_UNKNOWN_TOPIC : -1;
    }
    snprintf(name, sizeof name, "%s", t->name);
    partitions = t->partitions;
    replication = t->replication;
    retention_bytes = t->retention_bytes;
    parts = malloc((size_t)partitions * sizeof *parts);
    if (parts != NULL) {
        memcpy(parts, t->parts, (size_t)partitions * sizeof *parts);
    }
    pthread_mutex_unlock(&g_meta.mutex);

    if (parts == NULL) {
        return -1;
    }

    char *text = NULL;
    size_t len = 0;
    FILE *f = open_memstream(&text, &len);
    if (f == NULL) {
        free(parts);
        return -1;
    }

    fprintf(f, "topic %s partitions=%d replication=%d retention.bytes=%llu\n", name,
            partitions, replication, (unsigned long long)retention_bytes);
    for (int p = 0; p < partitions; p++) {
        fprintf(f, "  partition %d leader=%d replicas=", p, parts[p].leader);
        write_replicas(f, &parts[p]);

        int isr[META_MAX_REPLICAS];
        int n_isr = 0;
        int64_t hwm = -1;
        int64_t leo = -1;
        if (repl_partition_state != NULL
            && repl_partition_state(name, p, isr, &n_isr, &hwm, &leo) == MB_OK) {
            fprintf(f, " isr=");
            for (int k = 0; k < n_isr; k++) {
                if (k > 0) {
                    fputc(',', f);
                }
                fprintf(f, "%d", isr[k]);
            }
            fprintf(f, " hwm=%lld leo=%lld", (long long)hwm, (long long)leo);
        }
        fputc('\n', f);
    }

    int rc = (fclose(f) == 0) ? MB_OK : -1;
    if (rc == MB_OK) {
        snprintf(out, size, "%s", text);
    }
    free(text);
    free(parts);
    return rc;
}

int meta_get_snapshot(char **out, size_t *len)
{
    if (out == NULL || len == NULL) {
        return -1;
    }
    pthread_mutex_lock(&g_meta.mutex);
    int rc = -1;
    if (g_meta.ready) {
        rc = snapshot_build(&g_meta.table, out, len);
    }
    pthread_mutex_unlock(&g_meta.mutex);
    return rc;
}

int meta_apply_snapshot(const char *text)
{
    if (text == NULL) {
        return -1;
    }
    pthread_mutex_lock(&g_meta.mutex);
    if (!g_meta.ready) {
        pthread_mutex_unlock(&g_meta.mutex);
        return -1;
    }

    table_t new_table;
    memset(&new_table, 0, sizeof new_table);
    if (snapshot_parse(text, g_meta.size, &new_table) != 0) {
        pthread_mutex_unlock(&g_meta.mutex);
        return -1;
    }

    if (new_table.version <= g_meta.table.version && g_meta.table.count > 0) {
        table_free(&new_table);
        pthread_mutex_unlock(&g_meta.mutex);
        return MB_OK;
    }

    for (size_t i = 0; i < new_table.count; i++) {
        topic_t *nt = &new_table.topics[i];
        topic_t *ot = table_find(&g_meta.table, nt->name);
        if (ot != NULL && ot->logs != NULL) {
            nt->logs = ot->logs;
            ot->logs = NULL;
        } else {
            (void)open_logs(nt, g_meta.dir, g_meta.rank);
        }
    }

    table_free(&g_meta.table);
    g_meta.table = new_table;
    table_save(&g_meta.table);
    pthread_mutex_unlock(&g_meta.mutex);
    return MB_OK;
}