#define _XOPEN_SOURCE 700

#include "check.h"
#include "log.h"

#include <ftw.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define WRITERS 16
#define PER_WRITER 1000
#define TOTAL (WRITERS * PER_WRITER)

static int64_t g_offsets[WRITERS][PER_WRITER];
static atomic_int g_append_errors = 0;

typedef struct {
    partition_log_t *log;
    int id;
} writer_arg_t;

static int rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

static void on_crash(int sig)
{
    static const char msg[] =
        "\n✘ race-log: the process crashed, concurrent appends corrupted the index or the heap\n";
    (void)sig;
    ssize_t written = write(STDOUT_FILENO, msg, sizeof msg - 1);
    (void)written;
    _exit(EXIT_FAILURE);
}

static void *writer_main(void *p)
{
    writer_arg_t *a = p;
    for (int i = 0; i < PER_WRITER; i++) {
        char value[32];
        int len = snprintf(value, sizeof value, "t=%d i=%d", a->id, i);
        message_t m = {0};
        m.value = value;
        m.value_len = (uint32_t)len;
        int64_t base = -1;
        if (plog_append(a->log, &m, 1, &base) != MB_OK) {
            atomic_fetch_add(&g_append_errors, 1);
        }
        g_offsets[a->id][i] = base;
    }
    return NULL;
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);
    signal(SIGABRT, on_crash);
    signal(SIGSEGV, on_crash);
    check_header("test_race_log", "concurrent appends to one partition");
    check_section("unique offsets", "16 writers x 1000 messages: no gaps, repeats or torn records");

    char dir[64];
    snprintf(dir, sizeof dir, "/tmp/plog_race_XXXXXX");
    if (mkdtemp(dir) == NULL) {
        perror("mkdtemp");
        return EXIT_FAILURE;
    }
    partition_log_t *log = plog_open(dir, 0);
    if (log == NULL) {
        return EXIT_FAILURE;
    }

    pthread_t threads[WRITERS];
    writer_arg_t args[WRITERS];
    for (int i = 0; i < WRITERS; i++) {
        args[i].log = log;
        args[i].id = i;
        pthread_create(&threads[i], NULL, writer_main, &args[i]);
    }
    for (int i = 0; i < WRITERS; i++) {
        pthread_join(threads[i], NULL);
    }

    static int claimed[TOTAL];
    int out_of_range = 0;
    for (int t = 0; t < WRITERS; t++) {
        for (int i = 0; i < PER_WRITER; i++) {
            int64_t off = g_offsets[t][i];
            if (off < 0 || off >= TOTAL) {
                out_of_range++;
            } else {
                claimed[off]++;
            }
        }
    }
    int duplicates = 0;
    int holes = 0;
    for (int k = 0; k < TOTAL; k++) {
        if (claimed[k] > 1) {
            duplicates++;
        }
        if (claimed[k] == 0) {
            holes++;
        }
    }

    int64_t end = plog_end_offset(log);
    int64_t next = 0;
    bool readable = true;
    int mismatches = 0;
    while (next < end && readable) {
        message_t *msgs = NULL;
        size_t n = 0;
        if (plog_read(log, next, 1 << 20, false, &msgs, &n) != MB_OK || n == 0) {
            readable = false;
            break;
        }
        for (size_t k = 0; k < n; k++) {
            char text[32];
            int t = -1;
            int i = -1;
            size_t len = msgs[k].value_len;
            if (len >= sizeof text) {
                len = sizeof text - 1;
            }
            memcpy(text, msgs[k].value, len);
            text[len] = '\0';
            if (sscanf(text, "t=%d i=%d", &t, &i) != 2 || t < 0 || t >= WRITERS
                || i < 0 || i >= PER_WRITER || g_offsets[t][i] != next) {
                mismatches++;
            }
            next++;
        }
        plog_free_messages(msgs);
    }

    printf("  append errors: %d, out of range: %d, duplicated offsets: %d, holes: %d, mismatches: %d\n",
           atomic_load(&g_append_errors), out_of_range, duplicates, holes, mismatches);
    CHECK(atomic_load(&g_append_errors) == 0);
    CHECK(out_of_range == 0);
    CHECK(duplicates == 0);
    CHECK(holes == 0);
    CHECK(end == TOTAL);
    CHECK(readable);
    CHECK(mismatches == 0);

    plog_close(log);
    nftw(dir, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
    return check_summary("test_race_log");
}