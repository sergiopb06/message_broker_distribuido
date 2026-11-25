// mb_logdump: prints a partition log so the logs of several replicas can be compared.
//
//   mb_logdump <partition_dir> [hwm|leo] [limit]
//
// First line: "start=<s> hwm=<h> leo=<l>". Then one line per record below the limit
// (the HWM by default, or the LEO, or the explicit number if it is smaller):
//   <offset> <timestamp_ms> <key in hex or -> <value in hex>
// Every byte of every record is printed, so two replicas whose dumps are equal hold
// byte-for-byte the same records. Run it only while no broker uses the directory.

#define _POSIX_C_SOURCE 200809L

#include "log.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_hex(const char *p, uint32_t n)
{
    static const char digits[] = "0123456789abcdef";
    for (uint32_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)p[i];
        putchar(digits[c >> 4]);
        putchar(digits[c & 0xF]);
    }
}

int main(int argc, char **argv)
{
    if (argc < 2 || argc > 4) {
        fprintf(stderr, "usage: %s <partition_dir> [hwm|leo] [limit]\n", argv[0]);
        return 2;
    }
    bool upto_leo = (argc >= 3 && strcmp(argv[2], "leo") == 0);
    if (argc >= 3 && !upto_leo && strcmp(argv[2], "hwm") != 0) {
        fprintf(stderr, "second argument must be hwm or leo\n");
        return 2;
    }

    partition_log_t *log = plog_open(argv[1], 0);
    if (log == NULL) {
        fprintf(stderr, "cannot open %s: %s\n", argv[1], strerror(errno));
        return 1;
    }
    int64_t start = plog_start_offset(log);
    int64_t hwm = plog_hwm(log);
    int64_t leo = plog_end_offset(log);
    int64_t limit = upto_leo ? leo : hwm;
    if (argc == 4) {
        long long explicit_limit = strtoll(argv[3], NULL, 10);
        if (explicit_limit < limit) {
            limit = explicit_limit;
        }
    }
    printf("start=%lld hwm=%lld leo=%lld\n", (long long)start, (long long)hwm, (long long)leo);

    int rc = 0;
    int64_t next = start;
    while (next < limit) {
        message_t *msgs = NULL;
        size_t n = 0;
        // committed_only=false: the limit is applied here, so the LEO mode works too.
        if (plog_read(log, next, 1u << 20, false, &msgs, &n) != MB_OK || n == 0) {
            fprintf(stderr, "read failed at offset %lld\n", (long long)next);
            rc = 1;
            break;
        }
        for (size_t i = 0; i < n && next < limit; i++, next++) {
            const message_t *m = &msgs[i];
            printf("%lld %lld ", (long long)m->offset, (long long)m->timestamp_ms);
            if (m->key_len == 0) {
                putchar('-');
            } else {
                print_hex(m->key, m->key_len);
            }
            putchar(' ');
            print_hex(m->value, m->value_len);
            putchar('\n');
        }
        plog_free_messages(msgs);
    }
    plog_close(log);
    return rc;
}
