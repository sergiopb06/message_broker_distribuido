#define _POSIX_C_SOURCE 200809L

#include "log.h"
#include "fsutil.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define RECORD_HEADER_BYTES 32
#define RECORD_BODY_FIXED 28
#define INITIAL_SEGMENTS 4
#define INITIAL_INDEX 1024
#define HWM_FILE "hwm.checkpoint"

#ifdef SIN_PROTECCION
#define APPEND_LOCK(log) ((void)(log))
#define APPEND_UNLOCK(log) ((void)(log))
#else
#define APPEND_LOCK(log) pthread_mutex_lock(&(log)->mutex)
#define APPEND_UNLOCK(log) pthread_mutex_unlock(&(log)->mutex)
#endif

typedef struct {
    int64_t base_offset;
    int fd;
    uint64_t size;
    int64_t count;
    uint64_t *pos; // pos[i] = file position of record base_offset + i
    size_t pos_cap;
} segment_t;

struct partition_log {
    char dir[PATH_MAX];
    uint64_t retention_bytes;
    uint64_t segment_bytes;
    pthread_mutex_t mutex;
    pthread_cond_t changed; // new data, new hwm or close
    pthread_cond_t idle;    // users reached zero
    segment_t *segs;
    size_t nsegs;
    size_t segs_cap;
    int64_t leo;
    int64_t hwm;
    int users;
    bool closed;
};

static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) {
            if (c & 1u) {
                c = 0xEDB88320u ^ (c >> 1);
            } else {
                c = c >> 1;
            }
        }
        crc_table[i] = c;
    }
}

static uint32_t crc32_calc(const uint8_t *data, size_t len)
{
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        c = crc_table[(c ^ data[i]) & 0xFFu] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

static void put_u32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++) {
        p[i] = (uint8_t)(v >> (8 * i));
    }
}

static void put_u64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) {
        p[i] = (uint8_t)(v >> (8 * i));
    }
}

static uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; i++) {
        v |= (uint32_t)p[i] << (8 * i);
    }
    return v;
}

static uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v |= (uint64_t)p[i] << (8 * i);
    }
    return v;
}

static int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static size_t record_bytes(const message_t *m)
{
    return RECORD_HEADER_BYTES + (size_t)m->key_len + (size_t)m->value_len;
}

static void encode_record(uint8_t *dst, const message_t *m, int64_t offset, int64_t timestamp)
{
    uint32_t body = RECORD_BODY_FIXED + m->key_len + m->value_len;
    put_u32(dst, body);
    put_u64(dst + 8, (uint64_t)offset);
    put_u64(dst + 16, (uint64_t)timestamp);
    put_u32(dst + 24, m->key_len);
    put_u32(dst + 28, m->value_len);
    if (m->key_len > 0) {
        memcpy(dst + RECORD_HEADER_BYTES, m->key, m->key_len);
    }
    if (m->value_len > 0) {
        memcpy(dst + RECORD_HEADER_BYTES + m->key_len, m->value, m->value_len);
    }
    put_u32(dst + 4, crc32_calc(dst + 8, body - 4));
}

static int decode_record(uint8_t *src, size_t avail, message_t *out, size_t *used)
{
    if (avail < RECORD_HEADER_BYTES) {
        return -1;
    }
    uint32_t length = get_u32(src);
    if (length < RECORD_BODY_FIXED || (size_t)length > avail - 4) {
        return -1;
    }
    uint32_t key_len = get_u32(src + 24);
    uint32_t value_len = get_u32(src + 28);
    if ((uint64_t)key_len + value_len + RECORD_BODY_FIXED != length) {
        return -1;
    }
    if (crc32_calc(src + 8, length - 4) != get_u32(src + 4)) {
        return -1;
    }
    out->offset = (int64_t)get_u64(src + 8);
    out->timestamp_ms = (int64_t)get_u64(src + 16);
    out->key_len = key_len;
    out->value_len = value_len;
    out->key = NULL;
    if (key_len > 0) {
        out->key = (char *)(src + RECORD_HEADER_BYTES);
    }
    out->value = (char *)(src + RECORD_HEADER_BYTES + key_len);
    *used = 4 + (size_t)length;
    return 0;
}

static int write_full(int fd, const uint8_t *buf, size_t len, uint64_t pos)
{
    size_t done = 0;
    while (done < len) {
        ssize_t w = pwrite(fd, buf + done, len - done, (off_t)(pos + done));
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        done += (size_t)w;
    }
    return 0;
}

static int read_full(int fd, uint8_t *buf, size_t len, uint64_t pos)
{
    size_t done = 0;
    while (done < len) {
        ssize_t r = pread(fd, buf + done, len - done, (off_t)(pos + done));
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (r == 0) {
            return -1;
        }
        done += (size_t)r;
    }
    return 0;
}

static int validate(const message_t *msgs, size_t n, size_t *total)
{
    size_t sum = 0;
    for (size_t i = 0; i < n; i++) {
        const message_t *m = &msgs[i];
        if ((uint64_t)m->key_len + m->value_len > PLOG_MAX_MESSAGE_BYTES) {
            return -1;
        }
        if (m->key_len > 0 && m->key == NULL) {
            return -1;
        }
        if (m->value_len > 0 && m->value == NULL) {
            return -1;
        }
        sum += record_bytes(m);
    }
    *total = sum;
    return 0;
}

static int segment_add_locked(partition_log_t *log, int64_t base, bool existing)
{
    if (log->nsegs == log->segs_cap) {
        size_t cap = log->segs_cap * 2;
        segment_t *grown = realloc(log->segs, cap * sizeof *grown);
        if (grown == NULL) {
            return -1;
        }
        log->segs = grown;
        log->segs_cap = cap;
    }
    char path[PATH_MAX + 32];
    int len = snprintf(path, sizeof path, "%s/%020lld.log", log->dir, (long long)base);
    if (len < 0 || (size_t)len >= sizeof path) {
        return -1;
    }
    int flags = O_RDWR;
    if (!existing) {
        flags |= O_CREAT | O_EXCL;
    }
    int fd = open(path, flags, 0644);
    if (fd < 0) {
        return -1;
    }
    uint64_t *pos = malloc(INITIAL_INDEX * sizeof *pos);
    if (pos == NULL) {
        close(fd);
        if (!existing) {
            unlink(path);
        }
        return -1;
    }
    segment_t *seg = &log->segs[log->nsegs];
    seg->base_offset = base;
    seg->fd = fd;
    seg->size = 0;
    seg->count = 0;
    seg->pos = pos;
    seg->pos_cap = INITIAL_INDEX;
    log->nsegs++;
    return 0;
}

static int index_reserve(segment_t *seg, size_t extra)
{
    size_t need = (size_t)seg->count + extra;
    if (need <= seg->pos_cap) {
        return 0;
    }
    size_t cap = seg->pos_cap;
    while (cap < need) {
        cap *= 2;
    }
    uint64_t *grown = realloc(seg->pos, cap * sizeof *grown);
    if (grown == NULL) {
        return -1;
    }
    seg->pos = grown;
    seg->pos_cap = cap;
    return 0;
}

static size_t find_segment(const partition_log_t *log, int64_t offset)
{
    size_t lo = 0;
    size_t hi = log->nsegs - 1;
    while (lo < hi) {
        size_t mid = lo + (hi - lo + 1) / 2;
        if (log->segs[mid].base_offset <= offset) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    return lo;
}

static int64_t limit_locked(const partition_log_t *log, bool committed_only)
{
    if (committed_only) {
        return log->hwm;
    }
    return log->leo;
}

static void leave_locked(partition_log_t *log)
{
    log->users--;
    if (log->users == 0) {
        pthread_cond_broadcast(&log->idle);
    }
}

static void leave(partition_log_t *log)
{
    pthread_mutex_lock(&log->mutex);
    leave_locked(log);
    pthread_mutex_unlock(&log->mutex);
}

static int64_t parse_segment_name(const char *name)
{
    if (strlen(name) != 24 || strcmp(name + 20, ".log") != 0) {
        return -1;
    }
    int64_t base = 0;
    for (int i = 0; i < 20; i++) {
        if (name[i] < '0' || name[i] > '9') {
            return -1;
        }
        int digit = name[i] - '0';
        if (base > (INT64_MAX - digit) / 10) {
            return -1;
        }
        base = base * 10 + digit;
    }
    return base;
}

static int compare_bases(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    if (x < y) {
        return -1;
    }
    if (x > y) {
        return 1;
    }
    return 0;
}

static int list_segments(const char *dir, int64_t **bases, size_t *n)
{
    DIR *d = opendir(dir);
    if (d == NULL) {
        return -1;
    }
    int64_t *list = NULL;
    size_t count = 0;
    size_t cap = 0;
    struct dirent *e = readdir(d);
    while (e != NULL) {
        int64_t base = parse_segment_name(e->d_name);
        if (base >= 0) {
            if (count == cap) {
                size_t new_cap = 8;
                if (cap > 0) {
                    new_cap = cap * 2;
                }
                int64_t *grown = realloc(list, new_cap * sizeof *grown);
                if (grown == NULL) {
                    free(list);
                    closedir(d);
                    return -1;
                }
                list = grown;
                cap = new_cap;
            }
            list[count] = base;
            count++;
        }
        e = readdir(d);
    }
    closedir(d);
    if (count > 1) {
        qsort(list, count, sizeof *list, compare_bases);
    }
    *bases = list;
    *n = count;
    return 0;
}

static bool record_valid(int fd, uint8_t *buf, uint64_t pos, uint64_t remaining,
                         int64_t expected, size_t *used)
{
    if (remaining < RECORD_HEADER_BYTES || read_full(fd, buf, 4, pos) != 0) {
        return false;
    }
    uint32_t length = get_u32(buf);
    if (length < RECORD_BODY_FIXED || length > PLOG_MAX_MESSAGE_BYTES + RECORD_BODY_FIXED) {
        return false;
    }
    size_t total = 4 + (size_t)length;
    if (total > remaining || read_full(fd, buf, total, pos) != 0) {
        return false;
    }
    message_t m;
    if (decode_record(buf, total, &m, used) != 0) {
        return false;
    }
    return m.offset == expected;
}

static int segment_scan(segment_t *seg, bool last)
{
    struct stat st;
    if (fstat(seg->fd, &st) != 0) {
        return -1;
    }
    uint64_t file_size = (uint64_t)st.st_size;
    uint8_t *buf = malloc(PLOG_MAX_MESSAGE_BYTES + RECORD_HEADER_BYTES);
    if (buf == NULL) {
        return -1;
    }
    uint64_t pos = 0;
    int rc = 0;
    bool valid = true;
    while (valid && pos < file_size) {
        size_t used = 0;
        valid = record_valid(seg->fd, buf, pos, file_size - pos, seg->base_offset + seg->count,
                             &used);
        if (valid) {
            if (index_reserve(seg, 1) != 0) {
                rc = -1;
                break;
            }
            seg->pos[seg->count] = pos;
            seg->count++;
            pos += used;
        }
    }
    free(buf);
    seg->size = pos;
    if (rc == 0 && pos < file_size) {
        if (!last) {
            errno = EIO;
            rc = -1;
        } else if (ftruncate(seg->fd, (off_t)pos) != 0 || fsync(seg->fd) != 0) {
            rc = -1;
        }
    }
    return rc;
}

static int recover_segments(partition_log_t *log, const int64_t *bases, size_t n)
{
    if (n == 0) {
        return segment_add_locked(log, 0, false);
    }
    for (size_t i = 0; i < n; i++) {
        if (i > 0) {
            const segment_t *prev = &log->segs[i - 1];
            if (bases[i] != prev->base_offset + prev->count) {
                errno = EIO;
                return -1;
            }
        }
        if (segment_add_locked(log, bases[i], true) != 0) {
            return -1;
        }
        if (segment_scan(&log->segs[i], i == n - 1) != 0) {
            return -1;
        }
    }
    const segment_t *last = &log->segs[n - 1];
    log->leo = last->base_offset + last->count;
    return 0;
}

static int hwm_save(const char *dir, int64_t hwm)
{
    char path[PATH_MAX + 32];
    char tmp[PATH_MAX + 32];
    int a = snprintf(path, sizeof path, "%s/%s", dir, HWM_FILE);
    int b = snprintf(tmp, sizeof tmp, "%s/%s.tmp", dir, HWM_FILE);
    if (a < 0 || b < 0 || (size_t)a >= sizeof path || (size_t)b >= sizeof tmp) {
        return -1;
    }
    char text[32];
    int len = snprintf(text, sizeof text, "%lld\n", (long long)hwm);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return -1;
    }
    int rc = 0;
    if (write_full(fd, (const uint8_t *)text, (size_t)len, 0) != 0 || fsync(fd) != 0) {
        rc = -1;
    }
    close(fd);
    if (rc == 0 && rename(tmp, path) != 0) {
        rc = -1;
    }
    return rc;
}

static int64_t hwm_load(const char *dir)
{
    char path[PATH_MAX + 32];
    int len = snprintf(path, sizeof path, "%s/%s", dir, HWM_FILE);
    if (len < 0 || (size_t)len >= sizeof path) {
        return 0;
    }
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return 0;
    }
    char text[32];
    ssize_t got = read(fd, text, sizeof text - 1);
    close(fd);
    if (got <= 0) {
        return 0;
    }
    text[got] = '\0';
    return (int64_t)strtoll(text, NULL, 10);
}

static void log_free(partition_log_t *log)
{
    for (size_t i = 0; i < log->nsegs; i++) {
        close(log->segs[i].fd);
        free(log->segs[i].pos);
    }
    free(log->segs);
    pthread_mutex_destroy(&log->mutex);
    pthread_cond_destroy(&log->changed);
    pthread_cond_destroy(&log->idle);
    free(log);
}

// Drops whole segments, oldest first, while the partition is above retention_bytes.
// Never drops the active segment or data at or above the high watermark.
// Readers hold their own dup() of the descriptor, so a dropped file stays readable for them.
static void apply_retention_locked(partition_log_t *log)
{
    if (log->retention_bytes == 0) {
        return;
    }
    uint64_t total = 0;
    for (size_t i = 0; i < log->nsegs; i++) {
        total += log->segs[i].size;
    }
    size_t drop = 0;
    while (drop + 1 < log->nsegs && total > log->retention_bytes
           && log->segs[drop + 1].base_offset <= log->hwm) {
        total -= log->segs[drop].size;
        drop++;
    }
    for (size_t i = 0; i < drop; i++) {
        char path[PATH_MAX + 32];
        int len = snprintf(path, sizeof path, "%s/%020lld.log", log->dir,
                           (long long)log->segs[i].base_offset);
        if (len < 0 || (size_t)len >= sizeof path || (unlink(path) != 0 && errno != ENOENT)) {
            drop = i;
            break;
        }
    }
    if (drop == 0) {
        return;
    }
    for (size_t i = 0; i < drop; i++) {
        close(log->segs[i].fd);
        free(log->segs[i].pos);
    }
    memmove(log->segs, log->segs + drop, (log->nsegs - drop) * sizeof *log->segs);
    log->nsegs -= drop;
}

partition_log_t *plog_open(const char *dir, uint64_t retention_bytes)
{
    // Retention drops whole segments, so they must be much smaller than the
    // limit: a quarter of it, between PLOG_MIN_SEGMENT_BYTES and the default.
    uint64_t segment_bytes = PLOG_DEFAULT_SEGMENT_BYTES;
    if (retention_bytes > 0 && retention_bytes / 4 < segment_bytes) {
        segment_bytes = retention_bytes / 4;
        if (segment_bytes < PLOG_MIN_SEGMENT_BYTES) {
            segment_bytes = PLOG_MIN_SEGMENT_BYTES;
        }
    }
    return plog_open_ex(dir, retention_bytes, segment_bytes);
}

partition_log_t *plog_open_ex(const char *dir, uint64_t retention_bytes, uint64_t segment_bytes)
{
    pthread_once(&crc_once, crc_init);
    if (fs_mkdir_p(dir) != 0) {
        return NULL;
    }
    int64_t *bases = NULL;
    size_t nbases = 0;
    if (list_segments(dir, &bases, &nbases) != 0) {
        return NULL;
    }
    partition_log_t *log = calloc(1, sizeof *log);
    if (log == NULL) {
        free(bases);
        return NULL;
    }
    int len = snprintf(log->dir, sizeof log->dir, "%s", dir);
    if (len < 0 || (size_t)len >= sizeof log->dir) {
        free(bases);
        free(log);
        return NULL;
    }
    log->retention_bytes = retention_bytes;
    log->segment_bytes = segment_bytes;
    if (log->segment_bytes == 0) {
        log->segment_bytes = PLOG_DEFAULT_SEGMENT_BYTES;
    }
    pthread_condattr_t attr;
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    pthread_mutex_init(&log->mutex, NULL);
    pthread_cond_init(&log->changed, &attr);
    pthread_cond_init(&log->idle, NULL);
    pthread_condattr_destroy(&attr);
    log->segs = malloc(INITIAL_SEGMENTS * sizeof *log->segs);
    if (log->segs == NULL) {
        free(bases);
        log_free(log);
        return NULL;
    }
    log->segs_cap = INITIAL_SEGMENTS;
    int rc = recover_segments(log, bases, nbases);
    free(bases);
    if (rc != 0) {
        log_free(log);
        return NULL;
    }
    int64_t hwm = hwm_load(log->dir);
    if (hwm > log->leo) {
        hwm = log->leo;
    }
    if (hwm < log->segs[0].base_offset) {
        hwm = log->segs[0].base_offset;
    }
    log->hwm = hwm;
    apply_retention_locked(log);
    return log;
}

static int append_batch(partition_log_t *log, const message_t *msgs, size_t n, bool assign,
                        int64_t *base_offset)
{
    size_t total = 0;
    if (log == NULL || n == 0 || validate(msgs, n, &total) != 0) {
        return -1;
    }
    uint8_t *buf = malloc(total);
    if (buf == NULL) {
        return -1;
    }
    int64_t stamp_now = now_ms();
    int rc = -1;
    segment_t *active = NULL;

    APPEND_LOCK(log);
    if (log->closed) {
        rc = MB_ERR_SHUTTING_DOWN;
        goto out;
    }
    if (!assign) {
        for (size_t i = 0; i < n; i++) {
            if (msgs[i].offset != log->leo + (int64_t)i) {
                rc = MB_ERR_OFFSET_OUT_OF_RANGE;
                goto out;
            }
        }
    }
    active = &log->segs[log->nsegs - 1];
    if (active->count > 0 && active->size + total > log->segment_bytes) {
        if (fsync(active->fd) != 0 || segment_add_locked(log, log->leo, false) != 0) {
            goto out;
        }
        active = &log->segs[log->nsegs - 1];
    }
    if (index_reserve(active, n) != 0) {
        goto out;
    }
    size_t at = 0;
    for (size_t i = 0; i < n; i++) {
        int64_t stamp = msgs[i].timestamp_ms;
        if (assign && stamp == 0) {
            stamp = stamp_now;
        }
        encode_record(buf + at, &msgs[i], log->leo + (int64_t)i, stamp);
        at += record_bytes(&msgs[i]);
    }
    if (write_full(active->fd, buf, total, active->size) != 0) {
        goto out;
    }
    uint64_t pos = active->size;
    for (size_t i = 0; i < n; i++) {
        active->pos[(size_t)active->count + i] = pos;
        pos += record_bytes(&msgs[i]);
    }
    active->size = pos;
    active->count += (int64_t)n;
    if (base_offset != NULL) {
        *base_offset = log->leo;
    }
    log->leo += (int64_t)n;
    apply_retention_locked(log);
    pthread_cond_broadcast(&log->changed);
    rc = MB_OK;
out:
    APPEND_UNLOCK(log);
    free(buf);
    return rc;
}

int plog_append(partition_log_t *log, const message_t *msgs, size_t n, int64_t *base_offset)
{
    return append_batch(log, msgs, n, true, base_offset);
}

int plog_append_replica(partition_log_t *log, const message_t *msgs, size_t n)
{
    return append_batch(log, msgs, n, false, NULL);
}

int plog_read(partition_log_t *log, int64_t from, size_t max_bytes, bool committed_only,
              message_t **out, size_t *n)
{
    *out = NULL;
    *n = 0;
    pthread_mutex_lock(&log->mutex);
    if (log->closed) {
        pthread_mutex_unlock(&log->mutex);
        return MB_ERR_SHUTTING_DOWN;
    }
    int64_t limit = limit_locked(log, committed_only);
    if (from < log->segs[0].base_offset) {
        from = log->segs[0].base_offset;
    }
    if (from > limit) {
        pthread_mutex_unlock(&log->mutex);
        return MB_ERR_OFFSET_OUT_OF_RANGE;
    }
    if (from == limit) {
        pthread_mutex_unlock(&log->mutex);
        return MB_OK;
    }
    const segment_t *seg = &log->segs[find_segment(log, from)];
    int64_t last = seg->base_offset + seg->count;
    if (limit < last) {
        last = limit;
    }
    size_t first = (size_t)(from - seg->base_offset);
    uint64_t start_pos = seg->pos[first];
    uint64_t end_pos = start_pos;
    size_t count = 0;
    for (size_t i = first; seg->base_offset + (int64_t)i < last; i++) {
        uint64_t record_end = seg->size;
        if ((int64_t)(i + 1) < seg->count) {
            record_end = seg->pos[i + 1];
        }
        if (count > 0 && record_end - start_pos > max_bytes) {
            break;
        }
        count++;
        end_pos = record_end;
    }
    int fd = dup(seg->fd);
    if (fd < 0) {
        pthread_mutex_unlock(&log->mutex);
        return -1;
    }
    log->users++;
    pthread_mutex_unlock(&log->mutex);

    int rc = -1;
    size_t bytes = (size_t)(end_pos - start_pos);
    message_t *msgs = malloc(count * sizeof *msgs + bytes);
    if (msgs != NULL) {
        uint8_t *raw = (uint8_t *)(msgs + count);
        if (read_full(fd, raw, bytes, start_pos) == 0) {
            rc = MB_OK;
            size_t at = 0;
            for (size_t i = 0; i < count && rc == MB_OK; i++) {
                size_t used = 0;
                if (decode_record(raw + at, bytes - at, &msgs[i], &used) != 0
                    || msgs[i].offset != from + (int64_t)i) {
                    rc = -1;
                }
                at += used;
            }
        }
        if (rc == MB_OK) {
            *out = msgs;
            *n = count;
        } else {
            free(msgs);
        }
    }
    close(fd);
    leave(log);
    return rc;
}

void plog_free_messages(message_t *msgs)
{
    free(msgs);
}

int plog_wait_data(partition_log_t *log, int64_t from, bool committed_only, int timeout_ms)
{
    struct timespec deadline;
    if (timeout_ms > 0) {
        clock_gettime(CLOCK_MONOTONIC, &deadline);
        deadline.tv_sec += timeout_ms / 1000;
        deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }
    }
    pthread_mutex_lock(&log->mutex);
    if (log->closed) {
        pthread_mutex_unlock(&log->mutex);
        return MB_ERR_SHUTTING_DOWN;
    }
    log->users++;
    int rc = MB_OK;
    bool timed_out = false;
    for (;;) {
        if (log->closed) {
            rc = MB_ERR_SHUTTING_DOWN;
            break;
        }
        if (limit_locked(log, committed_only) > from) {
            rc = MB_OK;
            break;
        }
        if (timeout_ms == 0 || timed_out) {
            rc = MB_ERR_TIMEOUT;
            break;
        }
        if (timeout_ms < 0) {
            pthread_cond_wait(&log->changed, &log->mutex);
        } else if (pthread_cond_timedwait(&log->changed, &log->mutex, &deadline) == ETIMEDOUT) {
            timed_out = true;
        }
    }
    leave_locked(log);
    pthread_mutex_unlock(&log->mutex);
    return rc;
}

int64_t plog_start_offset(partition_log_t *log)
{
    pthread_mutex_lock(&log->mutex);
    int64_t start = log->segs[0].base_offset;
    pthread_mutex_unlock(&log->mutex);
    return start;
}

int64_t plog_end_offset(partition_log_t *log)
{
    pthread_mutex_lock(&log->mutex);
    int64_t leo = log->leo;
    pthread_mutex_unlock(&log->mutex);
    return leo;
}

int64_t plog_hwm(partition_log_t *log)
{
    pthread_mutex_lock(&log->mutex);
    int64_t hwm = log->hwm;
    pthread_mutex_unlock(&log->mutex);
    return hwm;
}

void plog_set_hwm(partition_log_t *log, int64_t hwm)
{
    pthread_mutex_lock(&log->mutex);
    if (hwm > log->leo) {
        hwm = log->leo;
    }
    if (hwm > log->hwm) {
        log->hwm = hwm;
        apply_retention_locked(log);
        pthread_cond_broadcast(&log->changed);
    }
    pthread_mutex_unlock(&log->mutex);
}

int plog_reset_replica(partition_log_t *log, int64_t next_offset)
{
    if (log == NULL || next_offset < 0) {
        return MB_ERR_INVALID_ARGUMENT;
    }

    pthread_mutex_lock(&log->mutex);
    if (log->closed) {
        pthread_mutex_unlock(&log->mutex);
        return MB_ERR_SHUTTING_DOWN;
    }
    // Moving backwards would reuse offsets, which the log contract forbids.
    if (next_offset < log->leo) {
        pthread_mutex_unlock(&log->mutex);
        return MB_ERR_INVALID_ARGUMENT;
    }

    // The data below next_offset was dropped by the leader's retention policy;
    // a replica has no consumer-visible state of its own (no leader election),
    // so the safe recovery is to start a fresh segment at next_offset.
    for (size_t i = 0; i < log->nsegs; i++) {
        char path[PATH_MAX + 32];
        int len = snprintf(path, sizeof path, "%s/%020lld.log", log->dir,
                           (long long)log->segs[i].base_offset);
        if (len >= 0 && (size_t)len < sizeof path) {
            (void)unlink(path);
        }
        close(log->segs[i].fd);
        free(log->segs[i].pos);
    }
    log->nsegs = 0;
    log->leo = next_offset;
    log->hwm = next_offset;

    int rc = segment_add_locked(log, next_offset, false);
    if (rc == 0 && hwm_save(log->dir, next_offset) != 0) {
        rc = -1;
    }
    pthread_cond_broadcast(&log->changed);
    pthread_mutex_unlock(&log->mutex);
    return (rc == 0) ? MB_OK : -1;
}

int plog_flush(partition_log_t *log)
{
    pthread_mutex_lock(&log->mutex);
    if (log->closed) {
        pthread_mutex_unlock(&log->mutex);
        return MB_ERR_SHUTTING_DOWN;
    }
    int fd = dup(log->segs[log->nsegs - 1].fd);
    if (fd < 0) {
        pthread_mutex_unlock(&log->mutex);
        return -1;
    }
    int64_t hwm = log->hwm;
    log->users++;
    pthread_mutex_unlock(&log->mutex);
    int rc = MB_OK;
    if (fsync(fd) != 0 || hwm_save(log->dir, hwm) != 0) {
        rc = -1;
    }
    close(fd);
    leave(log);
    return rc;
}

void plog_close(partition_log_t *log)
{
    if (log == NULL) {
        return;
    }
    pthread_mutex_lock(&log->mutex);
    log->closed = true;
    pthread_cond_broadcast(&log->changed);
    while (log->users > 0) {
        pthread_cond_wait(&log->idle, &log->mutex);
    }
    pthread_mutex_unlock(&log->mutex);

    fsync(log->segs[log->nsegs - 1].fd);
    hwm_save(log->dir, log->hwm);
    log_free(log);
}