#define _POSIX_C_SOURCE 200809L

#include "fsutil.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_READ_BYTES (64u * 1024u * 1024u)

int fs_mkdir_p(const char *path)
{
    char tmp[PATH_MAX];
    int written = snprintf(tmp, sizeof tmp, "%s", path);
    if (written < 0 || (size_t)written >= sizeof tmp) {
        errno = ENAMETOOLONG;
        return -1;
    }
    size_t len = strlen(tmp);
    if (len > 1 && tmp[len - 1] == '/') {
        tmp[len - 1] = '\0';
        len--;
    }
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0755) < 0 && errno != EEXIST) {
                return -1;
            }
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0755) < 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
}

int fs_write_atomic(const char *path, const void *buf, size_t len)
{
    char tmp[PATH_MAX + 8];
    int written = snprintf(tmp, sizeof tmp, "%s.tmp", path);
    if (written < 0 || (size_t)written >= sizeof tmp) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return -1;
    }
    const char *bytes = buf;
    size_t done = 0;
    while (done < len) {
        ssize_t w = write(fd, bytes + done, len - done);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            close(fd);
            return -1;
        }
        done += (size_t)w;
    }
    if (fsync(fd) != 0) {
        close(fd);
        return -1;
    }
    if (close(fd) != 0 || rename(tmp, path) != 0) {
        return -1;
    }
    return 0;
}

int fs_read_file(const char *path, char **out, size_t *len)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 0 || (uint64_t)st.st_size > MAX_READ_BYTES) {
        close(fd);
        errno = EFBIG;
        return -1;
    }
    size_t size = (size_t)st.st_size;
    char *buf = malloc(size + 1);
    if (buf == NULL) {
        close(fd);
        return -1;
    }
    size_t done = 0;
    while (done < size) {
        ssize_t r = read(fd, buf + done, size - done);
        if (r < 0 && errno == EINTR) {
            continue;
        }
        if (r <= 0) {
            break;
        }
        done += (size_t)r;
    }
    close(fd);
    buf[done] = '\0';
    *out = buf;
    *len = done;
    return 0;
}