#define _XOPEN_SOURCE 700

#include "check.h"
#include "log.h"

#include <dirent.h>
#include <ftw.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define WRITERS 8
#define WRITES_PER_WRITER 500

static atomic_int g_worker_errors = 0;

static int rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

static void rm_rf(const char *path)
{
    nftw(path, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
}

static void make_tmpdir(char *buf, size_t size)
{
    snprintf(buf, size, "/tmp/plog_XXXXXX");
    if (mkdtemp(buf) == NULL) {
        perror("mkdtemp");
        exit(EXIT_FAILURE);
    }
}

static int count_segments(const char *dir)
{
    DIR *d = opendir(dir);
    if (d == NULL) {
        return -1;
    }
    int count = 0;
    struct dirent *e = readdir(d);
    while (e != NULL) {
        size_t len = strlen(e->d_name);
        if (len > 4 && strcmp(e->d_name + len - 4, ".log") == 0) {
            count++;
        }
        e = readdir(d);
    }
    closedir(d);
    return count;
}

static void sleep_ms(int ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static message_t make_msg(const char *key, const char *value, size_t value_len)
{
    message_t m = {0};
    if (key != NULL) {
        m.key = (char *)key;
        m.key_len = (uint32_t)strlen(key);
    }
    m.value = (char *)value;
    m.value_len = (uint32_t)value_len;
    return m;
}

static bool same_bytes(const char *a, uint32_t a_len, const char *b, size_t b_len)
{
    return a_len == b_len && (b_len == 0 || memcmp(a, b, b_len) == 0);
}

static void test_append_and_read(void)
{
    check_section("append and read", "consecutive offsets from 0 and exact bytes back");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open(dir, 0);
    CHECK(log != NULL);

    const char binary[] = { 'a', '\0', (char)0xFF, 'z' };
    message_t first[3] = {
        make_msg("k1", "hello", 5),
        make_msg(NULL, binary, sizeof binary),
        make_msg("k2", "", 0),
    };
    int64_t base = -1;
    CHECK(plog_append(log, first, 3, &base) == MB_OK);
    CHECK(base == 0);
    message_t second[2] = { make_msg(NULL, "x", 1), make_msg("k3", "yy", 2) };
    CHECK(plog_append(log, second, 2, &base) == MB_OK);
    CHECK(base == 3);
    CHECK(plog_end_offset(log) == 5);

    message_t *out = NULL;
    size_t n = 0;
    CHECK(plog_read(log, 0, 1 << 20, false, &out, &n) == MB_OK);
    CHECK(n == 5);
    if (n == 5) {
        for (size_t i = 0; i < n; i++) {
            CHECK(out[i].offset == (int64_t)i);
        }
        CHECK(same_bytes(out[0].key, out[0].key_len, "k1", 2));
        CHECK(same_bytes(out[0].value, out[0].value_len, "hello", 5));
        CHECK(out[1].key == NULL && out[1].key_len == 0);
        CHECK(same_bytes(out[1].value, out[1].value_len, binary, sizeof binary));
        CHECK(out[2].value_len == 0);
        CHECK(same_bytes(out[4].value, out[4].value_len, "yy", 2));
        CHECK(out[0].timestamp_ms > 0);
    }
    plog_free_messages(out);

    CHECK(plog_read(log, 2, 1 << 20, false, &out, &n) == MB_OK);
    CHECK(n == 3 && out[0].offset == 2);
    plog_free_messages(out);

    plog_close(log);
    partition_log_t *again = plog_open(dir, 0);
    CHECK(again != NULL && plog_end_offset(again) == 5);
    plog_close(again);
    rm_rf(dir);
}

static void test_visibility(void)
{
    check_section("visibility", "committed reads stop at the high watermark");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open(dir, 0);
    message_t msgs[5];
    for (int i = 0; i < 5; i++) {
        msgs[i] = make_msg(NULL, "v", 1);
    }
    plog_append(log, msgs, 5, NULL);

    message_t *out = NULL;
    size_t n = 99;
    CHECK(plog_read(log, 0, 1 << 20, true, &out, &n) == MB_OK);
    CHECK(n == 0);
    plog_set_hwm(log, 3);
    CHECK(plog_read(log, 0, 1 << 20, true, &out, &n) == MB_OK);
    CHECK(n == 3);
    plog_free_messages(out);
    plog_set_hwm(log, 100);
    CHECK(plog_hwm(log) == 5);
    plog_set_hwm(log, 1);
    CHECK(plog_hwm(log) == 5);
    CHECK(plog_read(log, 6, 1 << 20, false, &out, &n) == MB_ERR_OFFSET_OUT_OF_RANGE);
    plog_close(log);
    rm_rf(dir);
}

static void test_segments(void)
{
    check_section("segments", "the log rolls into several files and reads cross them");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 0, 256);
    char value[40];
    memset(value, 'q', sizeof value);
    int failed = 0;
    for (int i = 0; i < 100; i++) {
        message_t m = make_msg(NULL, value, sizeof value);
        int64_t base = -1;
        if (plog_append(log, &m, 1, &base) != MB_OK || base != i) {
            failed++;
        }
    }
    CHECK(failed == 0);
    CHECK(count_segments(dir) > 1);

    int64_t next = 0;
    int reads = 0;
    bool ordered = true;
    while (next < 100 && reads < 1000) {
        message_t *out = NULL;
        size_t n = 0;
        if (plog_read(log, next, 100, false, &out, &n) != MB_OK || n == 0) {
            ordered = false;
            break;
        }
        for (size_t i = 0; i < n; i++) {
            if (out[i].offset != next) {
                ordered = false;
            }
            next++;
        }
        plog_free_messages(out);
        reads++;
    }
    CHECK(ordered);
    CHECK(next == 100);
    CHECK(reads > 1);
    plog_close(log);
    rm_rf(dir);
}

static void test_replica_append(void)
{
    check_section("replica append", "replicas keep the leader offsets and reject gaps");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open(dir, 0);
    message_t msgs[3] = { make_msg(NULL, "a", 1), make_msg(NULL, "b", 1), make_msg(NULL, "c", 1) };
    for (int i = 0; i < 3; i++) {
        msgs[i].offset = i;
        msgs[i].timestamp_ms = 1000 + i;
    }
    CHECK(plog_append_replica(log, msgs, 2) == MB_OK);
    CHECK(plog_append_replica(log, &msgs[2], 1) == MB_OK);
    CHECK(plog_end_offset(log) == 3);
    message_t gap = make_msg(NULL, "d", 1);
    gap.offset = 7;
    CHECK(plog_append_replica(log, &gap, 1) == MB_ERR_OFFSET_OUT_OF_RANGE);
    CHECK(plog_end_offset(log) == 3);

    message_t *out = NULL;
    size_t n = 0;
    CHECK(plog_read(log, 0, 1 << 20, false, &out, &n) == MB_OK);
    CHECK(n == 3 && out[2].timestamp_ms == 1002);
    plog_free_messages(out);
    plog_close(log);
    rm_rf(dir);
}

typedef struct {
    partition_log_t *log;
    int delay_ms;
    bool set_hwm;
} delayed_arg_t;

static void *delayed_append(void *p)
{
    delayed_arg_t *a = p;
    sleep_ms(a->delay_ms);
    message_t m = make_msg(NULL, "late", 4);
    plog_append(a->log, &m, 1, NULL);
    if (a->set_hwm) {
        sleep_ms(a->delay_ms);
        plog_set_hwm(a->log, plog_end_offset(a->log));
    }
    return NULL;
}

static void test_wait_data(void)
{
    check_section("wait for data", "readers sleep on a condition variable until data or timeout");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open(dir, 0);

    CHECK(plog_wait_data(log, 0, false, 0) == MB_ERR_TIMEOUT);
    CHECK(plog_wait_data(log, 0, false, 50) == MB_ERR_TIMEOUT);

    pthread_t t;
    delayed_arg_t arg = { log, 50, false };
    pthread_create(&t, NULL, delayed_append, &arg);
    CHECK(plog_wait_data(log, 0, false, 5000) == MB_OK);
    pthread_join(t, NULL);

    arg.set_hwm = true;
    pthread_create(&t, NULL, delayed_append, &arg);
    CHECK(plog_wait_data(log, 0, true, 5000) == MB_OK);
    CHECK(plog_hwm(log) == 2);
    pthread_join(t, NULL);

    plog_close(log);
    rm_rf(dir);
}

typedef struct {
    partition_log_t *log;
    int result;
} waiter_arg_t;

static void *forever_waiter(void *p)
{
    waiter_arg_t *a = p;
    a->result = plog_wait_data(a->log, 0, true, -1);
    return NULL;
}

static void test_close_wakes(void)
{
    check_section("close", "closing wakes a reader that waits forever");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open(dir, 0);
    waiter_arg_t arg = { log, -1 };
    pthread_t t;
    pthread_create(&t, NULL, forever_waiter, &arg);
    sleep_ms(200);
    plog_close(log);
    pthread_join(t, NULL);
    CHECK(arg.result == MB_ERR_SHUTTING_DOWN);
    rm_rf(dir);
}

typedef struct {
    partition_log_t *log;
    int id;
} writer_arg_t;

static void *writer_main(void *p)
{
    writer_arg_t *a = p;
    for (int i = 0; i < WRITES_PER_WRITER; i++) {
        char value[32];
        int len = snprintf(value, sizeof value, "w=%d i=%d", a->id, i);
        message_t m = make_msg(NULL, value, (size_t)len);
        if (plog_append(a->log, &m, 1, NULL) != MB_OK) {
            atomic_fetch_add(&g_worker_errors, 1);
        }
    }
    return NULL;
}

typedef struct {
    partition_log_t *log;
    int64_t total;
    int64_t read;
    bool ok;
} reader_arg_t;

static void *reader_main(void *p)
{
    reader_arg_t *a = p;
    int last[WRITERS];
    for (int i = 0; i < WRITERS; i++) {
        last[i] = -1;
    }
    int64_t next = 0;
    a->ok = true;
    while (next < a->total && a->ok) {
        if (plog_wait_data(a->log, next, false, 5000) != MB_OK) {
            a->ok = false;
            break;
        }
        message_t *out = NULL;
        size_t n = 0;
        if (plog_read(a->log, next, 4096, false, &out, &n) != MB_OK) {
            a->ok = false;
            break;
        }
        for (size_t i = 0; i < n; i++) {
            char text[32];
            int id = -1;
            int seq = -1;
            size_t len = out[i].value_len;
            if (len >= sizeof text) {
                len = sizeof text - 1;
            }
            memcpy(text, out[i].value, len);
            text[len] = '\0';
            if (out[i].offset != next || sscanf(text, "w=%d i=%d", &id, &seq) != 2
                || id < 0 || id >= WRITERS || seq != last[id] + 1) {
                a->ok = false;
                break;
            }
            last[id] = seq;
            next++;
        }
        plog_free_messages(out);
    }
    a->read = next;
    return NULL;
}

static void test_concurrent_append(void)
{
    check_section("concurrent append", "many writers and a live reader: no gaps, repeats or torn records");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 0, 4096);
    int64_t total = (int64_t)WRITERS * WRITES_PER_WRITER;

    pthread_t reader;
    reader_arg_t rarg = { log, total, 0, false };
    pthread_create(&reader, NULL, reader_main, &rarg);

    pthread_t writers[WRITERS];
    writer_arg_t wargs[WRITERS];
    for (int i = 0; i < WRITERS; i++) {
        wargs[i].log = log;
        wargs[i].id = i;
        pthread_create(&writers[i], NULL, writer_main, &wargs[i]);
    }
    for (int i = 0; i < WRITERS; i++) {
        pthread_join(writers[i], NULL);
    }
    pthread_join(reader, NULL);

    CHECK(atomic_load(&g_worker_errors) == 0);
    CHECK(plog_end_offset(log) == total);
    CHECK(rarg.ok);
    CHECK(rarg.read == total);
    plog_close(log);
    rm_rf(dir);
}

int main(void)
{
    check_header("test_log", "partition log: offsets, format, segments, visibility and waiting");
    test_append_and_read();
    test_visibility();
    test_segments();
    test_replica_append();
    test_wait_data();
    test_close_wakes();
    test_concurrent_append();
    return check_summary("test_log");
}