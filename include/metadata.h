#ifndef METADATA_H
#define METADATA_H

#include <stddef.h>
#include <stdint.h>

int meta_create_topic(const char *name, int partitions, int replication, uint64_t retention_bytes);

int meta_list(char *out, size_t size);

int meta_describe(const char *topic, char *out, size_t size);

#endif //METADATA_H