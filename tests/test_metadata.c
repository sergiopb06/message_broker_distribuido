#define _XOPEN_SOURCE 700

#include "check.h"
#include "common.h"
#include "metadata.h"

#include <ftw.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BIG (1 << 20)
#define SAME_TOPIC_THREADS 16
#define CREATOR_THREADS 8
#define TOPICS_PER_THREAD 25

static char g_text[BIG];

static int rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

static void make_tmpdir(char *buf, size_t size)
{
    snprintf(buf, size, "/tmp/meta_XXXXXX");
    if (mkdtemp(buf) == NULL) {
        perror("mkdtemp");
        exit(EXIT_FAILURE);
    }
}

static void rm_rf(const char *path)
{
    nftw(path, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
}

static int reinit(int rank, int size, const char *dir)
{
    meta_destroy();
    return meta_init(rank, size, dir);
}

static int count_lines(const char *text)
{
    int lines = 0;
    for (size_t i = 0; text[i] != '\0'; i++) {
        if (text[i] == '\n') {
            lines++;
        }
    }
    return lines;
}

static int listed_topics(void)
{
    if (meta_list(g_text, sizeof g_text) != MB_OK) {
        return -1;
    }
    return count_lines(g_text);
}

static bool partition_is(const char *topic, int p, int leader, int n, const int *replicas)
{
    partition_info_t info;
    if (meta_get_partition(topic, p, &info) != MB_OK || info.leader != leader || info.n_replicas != n) {
        return false;
    }
    for (int k = 0; k < n; k++) {
        if (info.replicas[k] != replicas[k]) {
            return false;
        }
    }
    return true;
}

static void write_metadata(const char *dir, const char *text)
{
    char path[128];
    snprintf(path, sizeof path, "%s/metadata", dir);
    FILE *f = fopen(path, "w");
    if (f != NULL) {
        fputs(text, f);
        fclose(f);
    }
}

static void test_empty(const char *dir)
{
    check_section("empty cluster", "a fresh data directory has no topics");
    CHECK(reinit(0, 3, dir) == 0);
    CHECK(listed_topics() == 0);
    CHECK(meta_describe("pedidos", g_text, sizeof g_text) == MB_ERR_UNKNOWN_TOPIC);
    partition_info_t info;
    CHECK(meta_get_partition("pedidos", 0, &info) == MB_ERR_UNKNOWN_TOPIC);
}

static void test_assignment(const char *dir)
{
    check_section("round-robin", "leaders and replicas spread across brokers");
    CHECK(reinit(0, 3, dir) == 0);
    CHECK(meta_create_topic("pedidos", 3, 2, 0) == MB_OK);
    const int p0[] = { 0, 1 };
    const int p1[] = { 1, 2 };
    const int p2[] = { 2, 0 };
    CHECK(partition_is("pedidos", 0, 0, 2, p0));
    CHECK(partition_is("pedidos", 1, 1, 2, p1));
    CHECK(partition_is("pedidos", 2, 2, 2, p2));

    CHECK(meta_create_topic("pagos", 2, 3, 4096) == MB_OK);
    const int q0[] = { 1, 2, 0 };
    const int q1[] = { 2, 0, 1 };
    CHECK(partition_is("pagos", 0, 1, 3, q0));
    CHECK(partition_is("pagos", 1, 2, 3, q1));

    topic_info_t info;
    CHECK(meta_topic_info("pagos", &info) == MB_OK);
    CHECK(info.partitions == 2 && info.replication == 3 && info.retention_bytes == 4096);
    partition_info_t part;
    CHECK(meta_get_partition("pagos", 2, &part) == MB_ERR_INVALID_ARGUMENT);

    CHECK(meta_describe("pedidos", g_text, sizeof g_text) == MB_OK);
    CHECK(strstr(g_text, "topic pedidos partitions=3 replication=2") != NULL);
    CHECK(strstr(g_text, "partition 2 leader=2 replicas=2,0") != NULL);
    CHECK(listed_topics() == 2);
    CHECK(strstr(g_text, "pagos partitions=2 replication=3 retention.bytes=4096") != NULL);
}

static void test_validation(const char *dir)
{
    check_section("validation", "duplicates, R > N and bad names are refused and change nothing");
    CHECK(reinit(0, 3, dir) == 0);
    int before = listed_topics();
    CHECK(meta_create_topic("pedidos", 1, 1, 0) == MB_ERR_TOPIC_EXISTS);
    CHECK(meta_create_topic("nuevo", 3, 4, 0) == MB_ERR_INVALID_ARGUMENT);
    CHECK(meta_create_topic("nuevo", 0, 1, 0) == MB_ERR_INVALID_ARGUMENT);
    CHECK(meta_create_topic("nuevo", 1, 0, 0) == MB_ERR_INVALID_ARGUMENT);
    CHECK(meta_create_topic("", 1, 1, 0) == MB_ERR_INVALID_ARGUMENT);
    CHECK(meta_create_topic("a b", 1, 1, 0) == MB_ERR_INVALID_ARGUMENT);
    CHECK(meta_create_topic("..", 1, 1, 0) == MB_ERR_INVALID_ARGUMENT);
    CHECK(meta_create_topic("a/b", 1, 1, 0) == MB_ERR_INVALID_ARGUMENT);
    char long_name[META_NAME_MAX + 1];
    memset(long_name, 'x', META_NAME_MAX);
    long_name[META_NAME_MAX] = '\0';
    CHECK(meta_create_topic(long_name, 1, 1, 0) == MB_ERR_INVALID_ARGUMENT);
    CHECK(listed_topics() == before);
    long_name[META_NAME_MAX - 1] = '\0';
    CHECK(meta_create_topic(long_name, 1, 1, 0) == MB_OK);
}

static void test_not_controller(void)
{
    check_section("controller", "only broker 0 creates topics until forwarding exists");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    CHECK(reinit(1, 3, dir) == 0);
    CHECK(meta_create_topic("pedidos", 1, 1, 0) == MB_ERR_NOT_LEADER);
    CHECK(listed_topics() == 0);
    rm_rf(dir);
}

static void test_persistence(const char *dir)
{
    check_section("persistence", "topics and assignments survive a restart");
    CHECK(reinit(0, 3, dir) == 0);
    int before = listed_topics();
    CHECK(reinit(0, 3, dir) == 0);
    CHECK(listed_topics() == before);
    const int p2[] = { 2, 0 };
    CHECK(partition_is("pedidos", 2, 2, 2, p2));
    topic_info_t info;
    CHECK(meta_topic_info("pagos", &info) == MB_OK && info.retention_bytes == 4096);
    CHECK(meta_create_topic("despues", 1, 1, 0) == MB_OK);
    CHECK(reinit(2, 3, dir) == 0);
    CHECK(meta_topic_info("despues", &info) == MB_OK);
}

static void test_bad_files(void)
{
    check_section("bad metadata file", "an unreadable or inconsistent file stops the broker from starting");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    const char *bad[] = {
        "garbage\n",
        "version 1\ntopic t 1 1 0\n0 5 5\n",
        "version 1\ntopic t 2 1 0\n0 0 0\n",
        "version 1\ntopic t 1 2 0\n0 0 1,2\n",
        "version 1\ntopic t 1 2 0\n0 2 0,1\n",
        "version 1\ntopic t 1 1 0\n0 0 0\ntopic t 1 1 0\n0 0 0\n",
        "version 1\ntopic a/b 1 1 0\n0 0 0\n",
        "version 1\n0 0 0\n",
    };
    int refused = 0;
    for (size_t i = 0; i < sizeof bad / sizeof bad[0]; i++) {
        write_metadata(dir, bad[i]);
        if (reinit(0, 3, dir) != 0) {
            refused++;
        }
    }
    CHECK(refused == (int)(sizeof bad / sizeof bad[0]));
    CHECK(meta_create_topic("t", 1, 1, 0) == -1);

    write_metadata(dir, "version 4\ntopic t 1 1 0\n0 2 2\n");
    CHECK(reinit(0, 3, dir) == 0);
    const int r[] = { 2 };
    CHECK(partition_is("t", 0, 2, 1, r));
    CHECK(reinit(0, 2, dir) != 0);
    rm_rf(dir);
}

static void test_truncation(const char *dir)
{
    check_section("small buffer", "a full buffer tells the server to retry with a bigger one");
    CHECK(reinit(0, 3, dir) == 0);
    char small[10];
    CHECK(meta_list(small, sizeof small) == MB_OK);
    CHECK(strlen(small) == sizeof small - 1);
    CHECK(meta_describe("pedidos", small, sizeof small) == MB_OK);
    CHECK(strlen(small) == sizeof small - 1);
}

static pthread_barrier_t g_start;
static atomic_int g_created = 0;
static atomic_int g_exists = 0;
static atomic_int g_other = 0;

static void *same_topic_main(void *arg)
{
    (void)arg;
    pthread_barrier_wait(&g_start);
    int rc = meta_create_topic("carrera", 4, 2, 0);
    if (rc == MB_OK) {
        atomic_fetch_add(&g_created, 1);
    } else if (rc == MB_ERR_TOPIC_EXISTS) {
        atomic_fetch_add(&g_exists, 1);
    } else {
        atomic_fetch_add(&g_other, 1);
    }
    return NULL;
}

static void *creator_main(void *arg)
{
    int id = *(int *)arg;
    pthread_barrier_wait(&g_start);
    for (int i = 0; i < TOPICS_PER_THREAD; i++) {
        char name[32];
        snprintf(name, sizeof name, "t%d_%d", id, i);
        if (meta_create_topic(name, 2, 2, 0) != MB_OK) {
            atomic_fetch_add(&g_other, 1);
        }
    }
    return NULL;
}

static void test_concurrent_create(void)
{
    check_section("concurrent create", "the same topic is created once; distinct topics are all kept");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    CHECK(reinit(0, 3, dir) == 0);

    pthread_t threads[SAME_TOPIC_THREADS];
    pthread_barrier_init(&g_start, NULL, SAME_TOPIC_THREADS);
    for (int i = 0; i < SAME_TOPIC_THREADS; i++) {
        pthread_create(&threads[i], NULL, same_topic_main, NULL);
    }
    for (int i = 0; i < SAME_TOPIC_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    pthread_barrier_destroy(&g_start);
    CHECK(atomic_load(&g_created) == 1);
    CHECK(atomic_load(&g_exists) == SAME_TOPIC_THREADS - 1);

    int ids[CREATOR_THREADS];
    pthread_barrier_init(&g_start, NULL, CREATOR_THREADS);
    for (int i = 0; i < CREATOR_THREADS; i++) {
        ids[i] = i;
        pthread_create(&threads[i], NULL, creator_main, &ids[i]);
    }
    for (int i = 0; i < CREATOR_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    pthread_barrier_destroy(&g_start);
    int expected = 1 + CREATOR_THREADS * TOPICS_PER_THREAD;
    CHECK(atomic_load(&g_other) == 0);
    CHECK(listed_topics() == expected);
    CHECK(reinit(0, 3, dir) == 0);
    CHECK(listed_topics() == expected);
    rm_rf(dir);
}

int main(void)
{
    check_header("test_metadata", "cluster metadata: topics, assignment, validation and persistence");
    char dir[64];
    make_tmpdir(dir, sizeof dir);
    test_empty(dir);
    test_assignment(dir);
    test_validation(dir);
    test_not_controller();
    test_persistence(dir);
    test_bad_files();
    test_truncation(dir);
    test_concurrent_create();
    meta_destroy();
    rm_rf(dir);
    return check_summary("test_metadata");
}