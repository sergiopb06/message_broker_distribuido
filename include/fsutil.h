#ifndef FSUTIL_H
#define FSUTIL_H

#include <stddef.h>

int fs_mkdir_p(const char *path);

// Writes to <path>.tmp, syncs it and renames it over path.
int fs_write_atomic(const char *path, const void *buf, size_t len);

// The result is NUL-terminated and must be freed. Sets errno to ENOENT if path does not exist.
int fs_read_file(const char *path, char **out, size_t *len);

#endif