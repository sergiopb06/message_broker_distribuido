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
#include <sys/stat.h>
#include <time.h>

#define SEGMENT 256
#define RECORD 40
#define PER_SEGMENT 6
#define STRESS_MESSAGES 3000
#define READERS 4

static atomic_int g_writer_errors = 0;

static int rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

static void make_tmpdir(char *buf, size_t size)
{
    snprintf(buf, size, "/tmp/plog_ret_XXXXXX");
    if (mkdtemp(buf) == NULL) {
        perror("mkdtemp");
        exit(EXIT_FAILURE);
    }
}

static void rm_rf(const char *path)
{
    nftw(path, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
}

static long disk_bytes(const char *dir)
{
    DIR *d = opendir(dir);
    long total = 0;
    if (d == NULL) {
        return -1;
    }
    struct dirent *e = readdir(d);
    while (e != NULL) {
        size_t len = strlen(e->d_name);
        if (len > 4 && strcmp(e->d_name + len - 4, ".log") == 0) {
            char path[512];
            struct stat st;
            snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
            if (stat(path, &st) == 0) {
                total += (long)st.st_size;
            }
        }
        e = readdir(d);
    }
    closedir(d);
    return total;
}

static void format_value(char *buf, int64_t offset)
{
    char tmp[16];
    snprintf(tmp, sizeof tmp, "o%07lld", (long long)offset);
    memcpy(buf, tmp, 8);
}

static int append_one(partition_log_t *log, int64_t expected, bool advance_hwm)
{
    char value[8];
    format_value(value, expected);
    message_t m = {0};
    m.value = value;
    m.value_len = 8;
    int64_t base = -1;
    if (plog_append(log, &m, 1, &base) != MB_OK || base != expected) {
        return -1;
    }
    if (advance_hwm) {
        plog_set_hwm(log, base + 1);
    }
    return 0;
}

static int append_many(partition_log_t *log, int64_t first, int count, bool advance_hwm)
{
    int errors = 0;
    for (int i = 0; i < count; i++) {
        if (append_one(log, first + i, advance_hwm) != 0) {
            errors++;
        }
    }
    return errors;
}

static bool check_message(const message_t *m, int64_t offset)
{
    char expected[8];
    format_value(expected, offset);
    return m->offset == offset && m->value_len == 8 && memcmp(m->value, expected, 8) == 0;
}

static bool verify_from_start(partition_log_t *log)
{
    int64_t next = plog_start_offset(log);
    int64_t end = plog_end_offset(log);
    while (next < end) {
        message_t *msgs = NULL;
        size_t n = 0;
        if (plog_read(log, next, 1 << 20, false, &msgs, &n) != MB_OK || n == 0) {
            return false;
        }
        for (size_t i = 0; i < n; i++) {
            if (!check_message(&msgs[i], next)) {
                plog_free_messages(msgs);
                return false;
            }
            next++;
        }
        plog_free_messages(msgs);
    }
    return true;
}

static void test_basic(void)
{
    check_section("size limit", "old segments are dropped and offsets keep growing");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 1000, SEGMENT);
    CHECK(append_many(log, 0, 100, true) == 0);
    int64_t start = plog_start_offset(log);
    CHECK(start > 0);
    CHECK(start % PER_SEGMENT == 0);
    CHECK(plog_end_offset(log) == 100);
    CHECK(disk_bytes(dir) <= 1000);
    CHECK(disk_bytes(dir) == (100 - start) * RECORD);

    message_t *msgs = NULL;
    size_t n = 0;
    CHECK(plog_read(log, 0, 1 << 20, true, &msgs, &n) == MB_OK);
    CHECK(n > 0 && msgs[0].offset == start);
    plog_free_messages(msgs);
    CHECK(verify_from_start(log));
    CHECK(append_one(log, 100, true) == 0);
    plog_close(log);
    rm_rf(dir);
}

static void test_respects_hwm(void)
{
    check_section("high watermark", "nothing at or above the high watermark is dropped");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 500, SEGMENT);
    CHECK(append_many(log, 0, 60, false) == 0);
    CHECK(plog_start_offset(log) == 0);
    CHECK(disk_bytes(dir) == 60 * RECORD);

    plog_set_hwm(log, 30);
    CHECK(plog_start_offset(log) == 30);
    CHECK(verify_from_start(log));

    plog_set_hwm(log, 60);
    CHECK(plog_start_offset(log) == 48);
    CHECK(disk_bytes(dir) == 12 * RECORD);
    CHECK(verify_from_start(log));
    plog_close(log);
    rm_rf(dir);
}

static void test_unlimited(void)
{
    check_section("no limit", "retention_bytes = 0 keeps everything");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 0, SEGMENT);
    CHECK(append_many(log, 0, 120, true) == 0);
    CHECK(plog_start_offset(log) == 0);
    CHECK(disk_bytes(dir) == 120 * RECORD);
    plog_close(log);
    rm_rf(dir);
}

static void test_reopen(void)
{
    check_section("reopen", "the start offset survives a restart and a smaller limit applies on open");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 1000, SEGMENT);
    append_many(log, 0, 100, true);
    int64_t start = plog_start_offset(log);
    plog_close(log);

    log = plog_open_ex(dir, 1000, SEGMENT);
    CHECK(log != NULL);
    CHECK(plog_start_offset(log) == start);
    CHECK(plog_end_offset(log) == 100);
    CHECK(plog_hwm(log) == 100);
    CHECK(verify_from_start(log));
    CHECK(append_one(log, 100, true) == 0);
    plog_close(log);

    log = plog_open_ex(dir, 300, SEGMENT);
    CHECK(log != NULL);
    CHECK(plog_start_offset(log) > start);
    CHECK(disk_bytes(dir) <= 300);
    CHECK(verify_from_start(log));
    plog_close(log);
    rm_rf(dir);
}

typedef struct {
    partition_log_t *log;
    int delay_us;
    bool ok;
    int64_t skipped;
    int64_t received;
} reader_arg_t;

static void *writer_main(void *p)
{
    partition_log_t *log = p;
    struct timespec pause = { 0, 20000L };
    for (int i = 0; i < STRESS_MESSAGES; i++) {
        if (append_one(log, i, true) != 0) {
            atomic_fetch_add(&g_writer_errors, 1);
        }
        nanosleep(&pause, NULL);
    }
    return NULL;
}

static void *reader_main(void *p)
{
    reader_arg_t *a = p;
    int64_t next = 0;
    a->ok = true;
    while (next < STRESS_MESSAGES && a->ok) {
        if (plog_wait_data(a->log, next, true, 5000) != MB_OK) {
            a->ok = false;
            break;
        }
        message_t *msgs = NULL;
        size_t n = 0;
        if (plog_read(a->log, next, 300, true, &msgs, &n) != MB_OK || n == 0) {
            a->ok = false;
            break;
        }
        if (msgs[0].offset < next) {
            a->ok = false;
        }
        a->skipped += msgs[0].offset - next;
        next = msgs[0].offset;
        for (size_t i = 0; i < n && a->ok; i++) {
            if (!check_message(&msgs[i], next)) {
                a->ok = false;
            }
            next++;
            a->received++;
        }
        plog_free_messages(msgs);
        if (a->delay_us > 0) {
            struct timespec ts = { 0, (long)a->delay_us * 1000L };
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

static void test_concurrent(void)
{
    check_section("concurrent", "readers keep reading while retention drops segments under them");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 2000, SEGMENT);

    pthread_t readers[READERS];
    reader_arg_t args[READERS];
    for (int i = 0; i < READERS; i++) {
        memset(&args[i], 0, sizeof args[i]);
        args[i].log = log;
        args[i].delay_us = i * 1000;
        pthread_create(&readers[i], NULL, reader_main, &args[i]);
    }
    pthread_t writer;
    pthread_create(&writer, NULL, writer_main, log);
    pthread_join(writer, NULL);

    bool all_ok = true;
    int64_t skipped = 0;
    int64_t received = 0;
    for (int i = 0; i < READERS; i++) {
        pthread_join(readers[i], NULL);
        if (!args[i].ok || args[i].received + args[i].skipped != STRESS_MESSAGES) {
            all_ok = false;
        }
        skipped += args[i].skipped;
        received += args[i].received;
    }
    printf("  received: %lld, skipped by retention: %lld\n", (long long)received, (long long)skipped);
    CHECK(atomic_load(&g_writer_errors) == 0);
    CHECK(all_ok);
    CHECK(plog_end_offset(log) == STRESS_MESSAGES);
    CHECK(disk_bytes(dir) <= 2000);
    CHECK(verify_from_start(log));
    plog_close(log);
    rm_rf(dir);
}

typedef struct {
    partition_log_t *log;
    atomic_bool *stop;
    int64_t reads;
    int64_t bad;
} hammer_arg_t;

static void *hammer_main(void *p)
{
    hammer_arg_t *a = p;
    while (!atomic_load(a->stop)) {
        message_t *msgs = NULL;
        size_t n = 0;
        int rc = plog_read(a->log, 0, 1 << 20, true, &msgs, &n);
        if (rc != MB_OK) {
            a->bad++;
            continue;
        }
        for (size_t i = 0; i < n; i++) {
            if (!check_message(&msgs[i], msgs[0].offset + (int64_t)i)) {
                a->bad++;
                break;
            }
        }
        plog_free_messages(msgs);
        a->reads++;
    }
    return NULL;
}

static void test_oldest_segment(void)
{
    check_section("oldest segment", "readers hammer the segment that retention drops next");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 2 * SEGMENT, SEGMENT);
    atomic_bool stop = false;
    pthread_t readers[READERS];
    hammer_arg_t args[READERS];
    append_many(log, 0, 3 * PER_SEGMENT, true);
    for (int i = 0; i < READERS; i++) {
        memset(&args[i], 0, sizeof args[i]);
        args[i].log = log;
        args[i].stop = &stop;
        pthread_create(&readers[i], NULL, hammer_main, &args[i]);
    }
    int errors = append_many(log, 3 * PER_SEGMENT, 20000, true);
    atomic_store(&stop, true);
    int64_t reads = 0;
    int64_t bad = 0;
    for (int i = 0; i < READERS; i++) {
        pthread_join(readers[i], NULL);
        reads += args[i].reads;
        bad += args[i].bad;
    }
    printf("  reads: %lld, failed or corrupt: %lld\n", (long long)reads, (long long)bad);
    CHECK(errors == 0);
    CHECK(reads > 0);
    CHECK(bad == 0);
    plog_close(log);
    rm_rf(dir);
}

int main(void)
{
    check_header("test_log_retention", "partition log: retention.bytes with concurrent readers");
    test_basic();
    test_respects_hwm();
    test_unlimited();
    test_reopen();
    test_concurrent();
    test_oldest_segment();
    return check_summary("test_log_retention");
}