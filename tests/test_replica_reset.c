// Tests for plog_reset_replica: recovering a replica whose leader has already
// deleted, through retention, the offsets the replica was missing.

#define _XOPEN_SOURCE 700

#include "check.h"
#include "log.h"

#include <dirent.h>
#include <ftw.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define SMALL_SEGMENT 256

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

// Appends offsets [from, to) as a replica, with a value derived from the offset.
static int append_range(partition_log_t *log, int64_t from, int64_t to)
{
    for (int64_t off = from; off < to; off++) {
        char value[32];
        int n = snprintf(value, sizeof value, "value-%lld", (long long)off);
        message_t m = { .offset = off, .timestamp_ms = 1000 + off, .key_len = 0,
                        .value_len = (uint32_t)n, .key = NULL, .value = value };
        int rc = plog_append_replica(log, &m, 1);
        if (rc != MB_OK) {
            return rc;
        }
    }
    return MB_OK;
}

// Counts the segments left in the directory and the smallest base offset among them.
static size_t segments_in(const char *dir, long long *min_base)
{
    DIR *d = opendir(dir);
    size_t count = 0;
    *min_base = -1;
    if (d == NULL) {
        return 0;
    }
    for (struct dirent *e = readdir(d); e != NULL; e = readdir(d)) {
        size_t len = strlen(e->d_name);
        if (len > 4 && strcmp(e->d_name + len - 4, ".log") == 0) {
            long long base = strtoll(e->d_name, NULL, 10);
            if (*min_base < 0 || base < *min_base) {
                *min_base = base;
            }
            count++;
        }
    }
    closedir(d);
    return count;
}

static bool record_is(const message_t *m, int64_t off)
{
    char expected[32];
    int n = snprintf(expected, sizeof expected, "value-%lld", (long long)off);
    return m->offset == off && m->timestamp_ms == 1000 + off && m->value_len == (uint32_t)n
           && memcmp(m->value, expected, (size_t)n) == 0;
}

int main(void)
{
    check_header("test_replica_reset", "réplica que se reinicia en el primer offset del líder");

    char dir[] = "/tmp/plog_reset_XXXXXX";
    if (mkdtemp(dir) == NULL) {
        perror("mkdtemp");
        return EXIT_FAILURE;
    }

    partition_log_t *log = plog_open_ex(dir, 0, SMALL_SEGMENT);
    CHECK(log != NULL);
    CHECK(append_range(log, 0, 20) == MB_OK);
    plog_set_hwm(log, 20);
    long long min_base = -1;
    CHECK(segments_in(dir, &min_base) > 1);

    check_section("límites", "nunca se reutiliza un offset");
    CHECK(plog_reset_replica(log, 15) == MB_ERR_INVALID_ARGUMENT);
    CHECK(plog_reset_replica(log, -1) == MB_ERR_INVALID_ARGUMENT);
    CHECK(plog_end_offset(log) == 20);

    check_section("reinicio", "el log arranca vacío en el offset del líder");
    CHECK(plog_reset_replica(log, 50) == MB_OK);
    CHECK(plog_start_offset(log) == 50);
    CHECK(plog_end_offset(log) == 50);
    CHECK(plog_hwm(log) == 50);
    CHECK(segments_in(dir, &min_base) == 1);
    CHECK(min_base == 50);

    check_section("continuidad", "después del reinicio solo se aceptan offsets consecutivos");
    CHECK(append_range(log, 40, 41) == MB_ERR_OFFSET_OUT_OF_RANGE);
    CHECK(append_range(log, 50, 60) == MB_OK);
    CHECK(plog_end_offset(log) == 60);
    plog_set_hwm(log, 60);

    message_t *msgs = NULL;
    size_t n = 0;
    // A reader asking for an already-deleted offset continues from the first available one.
    CHECK(plog_read(log, 0, 1u << 20, true, &msgs, &n) == MB_OK);
    CHECK(n > 0 && record_is(&msgs[0], 50));
    plog_free_messages(msgs);

    check_section("persistencia", "el reinicio sobrevive a cerrar y reabrir");
    plog_close(log);
    log = plog_open_ex(dir, 0, SMALL_SEGMENT);
    CHECK(log != NULL);
    CHECK(plog_start_offset(log) == 50);
    CHECK(plog_end_offset(log) == 60);
    CHECK(plog_hwm(log) == 60);
    bool all_ok = true;
    for (int64_t off = 50; off < 60; off++) {
        msgs = NULL;
        n = 0;
        if (plog_read(log, off, 1u << 20, true, &msgs, &n) != MB_OK || n == 0
            || !record_is(&msgs[0], off)) {
            all_ok = false;
        }
        plog_free_messages(msgs);
    }
    CHECK(all_ok);
    plog_close(log);

    rm_rf(dir);
    return check_summary("test_replica_reset");
}
