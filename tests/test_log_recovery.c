#define _XOPEN_SOURCE 700

#include "check.h"
#include "log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define SMALL_SEGMENT 256

static int rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

static void make_tmpdir(char *buf, size_t size)
{
    snprintf(buf, size, "/tmp/plog_rec_XXXXXX");
    if (mkdtemp(buf) == NULL) {
        perror("mkdtemp");
        exit(EXIT_FAILURE);
    }
}

static void rm_rf(const char *path)
{
    nftw(path, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
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

static size_t list_bases(const char *dir, int64_t *out, size_t max)
{
    DIR *d = opendir(dir);
    size_t count = 0;
    if (d == NULL) {
        return 0;
    }
    struct dirent *e = readdir(d);
    while (e != NULL) {
        size_t len = strlen(e->d_name);
        if (len == 24 && strcmp(e->d_name + 20, ".log") == 0 && count < max) {
            out[count] = atoll(e->d_name);
            count++;
        }
        e = readdir(d);
    }
    closedir(d);
    qsort(out, count, sizeof *out, compare_bases);
    return count;
}

static void segment_path(char *buf, size_t size, const char *dir, int64_t base)
{
    snprintf(buf, size, "%s/%020lld.log", dir, (long long)base);
}

static long file_size(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    return (long)st.st_size;
}

static void flip_byte(const char *path, off_t pos)
{
    int fd = open(path, O_RDWR);
    unsigned char b = 0;
    if (fd < 0) {
        return;
    }
    if (pread(fd, &b, 1, pos) == 1) {
        b = (unsigned char)(b ^ 0x5A);
        ssize_t w = pwrite(fd, &b, 1, pos);
        (void)w;
    }
    close(fd);
}

static void append_bytes(const char *path, unsigned char value, size_t count)
{
    int fd = open(path, O_WRONLY | O_APPEND);
    unsigned char buf[64];
    if (fd < 0 || count > sizeof buf) {
        return;
    }
    memset(buf, value, count);
    ssize_t w = write(fd, buf, count);
    (void)w;
    close(fd);
}

static void write_text_file(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    if (f != NULL) {
        fputs(text, f);
        fclose(f);
    }
}

static void append_numbered(partition_log_t *log, int count)
{
    for (int i = 0; i < count; i++) {
        char value[16];
        snprintf(value, sizeof value, "p%07d", i);
        message_t m = {0};
        m.value = value;
        m.value_len = 8;
        plog_append(log, &m, 1, NULL);
    }
}

static void build_log(const char *dir, int count, uint64_t segment_bytes)
{
    partition_log_t *log = plog_open_ex(dir, 0, segment_bytes);
    if (log == NULL) {
        return;
    }
    append_numbered(log, count);
    plog_close(log);
}

static bool verify_log(partition_log_t *log)
{
    int64_t end = plog_end_offset(log);
    int64_t next = plog_start_offset(log);
    while (next < end) {
        message_t *msgs = NULL;
        size_t n = 0;
        if (plog_read(log, next, 1 << 20, false, &msgs, &n) != MB_OK || n == 0) {
            return false;
        }
        for (size_t k = 0; k < n; k++) {
            char expected[16];
            snprintf(expected, sizeof expected, "p%07d", (int)next);
            if (msgs[k].offset != next || msgs[k].value_len != 8
                || memcmp(msgs[k].value, expected, 8) != 0) {
                plog_free_messages(msgs);
                return false;
            }
            next++;
        }
        plog_free_messages(msgs);
    }
    return true;
}

static void test_reopen(void)
{
    check_section("reopen", "offsets, data and high watermark survive close and open");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open_ex(dir, 0, SMALL_SEGMENT);
    append_numbered(log, 50);
    plog_set_hwm(log, 30);
    plog_close(log);

    int64_t bases[64];
    CHECK(list_bases(dir, bases, 64) > 1);

    log = plog_open_ex(dir, 0, SMALL_SEGMENT);
    CHECK(log != NULL);
    CHECK(plog_start_offset(log) == 0);
    CHECK(plog_end_offset(log) == 50);
    CHECK(plog_hwm(log) == 30);
    CHECK(verify_log(log));

    message_t m = {0};
    m.value = "p0000050";
    m.value_len = 8;
    int64_t base = -1;
    CHECK(plog_append(log, &m, 1, &base) == MB_OK);
    CHECK(base == 50);
    CHECK(verify_log(log));
    plog_close(log);

    log = plog_open_ex(dir, 0, SMALL_SEGMENT);
    CHECK(log != NULL && plog_end_offset(log) == 51);
    plog_close(log);
    rm_rf(dir);
}

static void test_empty_reopen(void)
{
    check_section("empty log", "an empty partition reopens and starts at offset 0");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    partition_log_t *log = plog_open(dir, 0);
    plog_close(log);
    log = plog_open(dir, 0);
    CHECK(log != NULL);
    CHECK(plog_end_offset(log) == 0 && plog_hwm(log) == 0);
    message_t m = {0};
    m.value = "x";
    m.value_len = 1;
    int64_t base = -1;
    CHECK(plog_append(log, &m, 1, &base) == MB_OK && base == 0);
    plog_close(log);
    rm_rf(dir);
}

static void test_torn_tail(void)
{
    check_section("torn tail", "an incomplete final record is dropped and the log keeps working");
    char dir[64];
    char path[128];
    make_tmpdir(dir, sizeof dir);
    segment_path(path, sizeof path, dir, 0);

    build_log(dir, 10, 0);
    CHECK(file_size(path) == 400);
    CHECK(truncate(path, 395) == 0);
    partition_log_t *log = plog_open(dir, 0);
    CHECK(log != NULL && plog_end_offset(log) == 9);
    CHECK(file_size(path) == 360);
    message_t m = {0};
    m.value = "p0000009";
    m.value_len = 8;
    int64_t base = -1;
    CHECK(plog_append(log, &m, 1, &base) == MB_OK && base == 9);
    CHECK(verify_log(log));
    plog_close(log);
    rm_rf(dir);

    make_tmpdir(dir, sizeof dir);
    segment_path(path, sizeof path, dir, 0);
    build_log(dir, 10, 0);
    append_bytes(path, 0xAB, 7);
    log = plog_open(dir, 0);
    CHECK(log != NULL && plog_end_offset(log) == 10);
    CHECK(file_size(path) == 400);
    plog_close(log);

    append_bytes(path, 0xFF, 40);
    log = plog_open(dir, 0);
    CHECK(log != NULL && plog_end_offset(log) == 10);
    CHECK(file_size(path) == 400);
    CHECK(verify_log(log));
    plog_close(log);
    rm_rf(dir);
}

static void test_corrupt_record(void)
{
    check_section("corruption", "a bad checksum in the last segment drops that record and what follows");
    char dir[64];
    char path[128];
    make_tmpdir(dir, sizeof dir);
    segment_path(path, sizeof path, dir, 0);
    build_log(dir, 10, 0);
    flip_byte(path, 4 * 40 + 32 + 2);

    partition_log_t *log = plog_open(dir, 0);
    CHECK(log != NULL && plog_end_offset(log) == 4);
    CHECK(file_size(path) == 160);
    CHECK(verify_log(log));
    plog_close(log);
    rm_rf(dir);

    make_tmpdir(dir, sizeof dir);
    build_log(dir, 20, SMALL_SEGMENT);
    segment_path(path, sizeof path, dir, 0);
    flip_byte(path, 50);
    errno = 0;
    CHECK(plog_open_ex(dir, 0, SMALL_SEGMENT) == NULL);
    CHECK(errno == EIO);
    rm_rf(dir);
}

static void test_segment_gap(void)
{
    check_section("missing segment", "a gap between segment files is refused");
    char dir[64];
    char path[128];
    make_tmpdir(dir, sizeof dir);
    build_log(dir, 20, SMALL_SEGMENT);
    int64_t bases[64];
    size_t n = list_bases(dir, bases, 64);
    CHECK(n >= 3);
    segment_path(path, sizeof path, dir, bases[1]);
    CHECK(remove(path) == 0);
    errno = 0;
    CHECK(plog_open_ex(dir, 0, SMALL_SEGMENT) == NULL);
    CHECK(errno == EIO);
    rm_rf(dir);
}

static void test_hwm_checkpoint(void)
{
    check_section("checkpoint", "the stored high watermark is clamped to the recovered range");
    char dir[64];
    char path[128];
    make_tmpdir(dir, sizeof dir);
    build_log(dir, 10, 0);
    snprintf(path, sizeof path, "%s/hwm.checkpoint", dir);

    write_text_file(path, "999\n");
    partition_log_t *log = plog_open(dir, 0);
    CHECK(log != NULL && plog_hwm(log) == 10);
    plog_close(log);

    write_text_file(path, "-5\n");
    log = plog_open(dir, 0);
    CHECK(log != NULL && plog_hwm(log) == 0);
    plog_close(log);

    write_text_file(path, "garbage");
    log = plog_open(dir, 0);
    CHECK(log != NULL && plog_hwm(log) == 0);
    plog_close(log);

    CHECK(remove(path) == 0);
    log = plog_open(dir, 0);
    CHECK(log != NULL && plog_hwm(log) == 0);
    plog_close(log);
    rm_rf(dir);
}

static void test_random_corruption(void)
{
    check_section("random corruption", "any flipped byte is rejected or trimmed, never a crash");
    srand(12345);
    int opened = 0;
    int rejected = 0;
    int inconsistent = 0;
    for (int round = 0; round < 300; round++) {
        char dir[64];
        char path[128];
        int64_t bases[64];
        make_tmpdir(dir, sizeof dir);
        build_log(dir, 20, SMALL_SEGMENT);
        size_t n = list_bases(dir, bases, 64);
        size_t pick = (size_t)rand() % n;
        segment_path(path, sizeof path, dir, bases[pick]);
        long size = file_size(path);
        flip_byte(path, (off_t)(rand() % size));

        partition_log_t *log = plog_open_ex(dir, 0, SMALL_SEGMENT);
        if (log == NULL) {
            rejected++;
        } else {
            opened++;
            if (!verify_log(log) || plog_end_offset(log) > 20) {
                inconsistent++;
            }
            plog_close(log);
        }
        rm_rf(dir);
    }
    printf("  opened: %d, rejected: %d, inconsistent: %d\n", opened, rejected, inconsistent);
    CHECK(inconsistent == 0);
    CHECK(opened + rejected == 300);
    CHECK(rejected > 0 && opened > 0);
}

int main(void)
{
    check_header("test_log_recovery", "partition log: reopen, torn tails, corruption and checkpoint");
    test_reopen();
    test_empty_reopen();
    test_torn_tail();
    test_corrupt_record();
    test_segment_gap();
    test_hwm_checkpoint();
    test_random_corruption();
    return check_summary("test_log_recovery");
}