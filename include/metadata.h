#ifndef METADATA_H
#define METADATA_H

#include <stddef.h>
#include <stdint.h>

#define META_NAME_MAX 64
#define META_MAX_PARTITIONS 1024
#define META_MAX_REPLICAS 16

typedef struct {
    int leader;
    int n_replicas;
    int replicas[META_MAX_REPLICAS];
} partition_info_t;

typedef struct {
    int partitions;
    int replication;
    uint64_t retention_bytes;
} topic_info_t;

// Loads <data_dir>/metadata if it exists. Broker 0 is the controller.
int meta_init(int rank, int size, const char *data_dir);
void meta_destroy(void);

int meta_create_topic(const char *name, int partitions, int replication, uint64_t retention_bytes);

int meta_topic_info(const char *topic, topic_info_t *out);

int meta_get_partition(const char *topic, int partition, partition_info_t *out);

int meta_list(char *out, size_t size);

int meta_describe(const char *topic, char *out, size_t size);

#endif //METADATA_H