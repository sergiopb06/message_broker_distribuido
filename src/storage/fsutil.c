#define _POSIX_C_SOURCE 200809L

#include "fsutil.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

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