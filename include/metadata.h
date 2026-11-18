#ifndef METADATA_H
#define METADATA_H

#include "log.h"

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

// Loads <data_dir>/metadata if it exists and opens the logs of the partitions this broker keeps,
// in <data_dir>/topics/<topic>/<partition>. Broker 0 is the controller.
int meta_init(int rank, int size, const char *data_dir);

// Closes every partition log. Call it once no thread uses them.
void meta_destroy(void);

int meta_create_topic(const char *name, int partitions, int replication, uint64_t retention_bytes);

int meta_topic_info(const char *topic, topic_info_t *out);

int meta_get_partition(const char *topic, int partition, partition_info_t *out);

// Log of the partition if this broker keeps it (leader or replica), or NULL.
// It stays valid until meta_destroy.
partition_log_t *meta_get_log(const char *topic, int partition);

int meta_list(char *out, size_t size);

int meta_describe(const char *topic, char *out, size_t size);

#endif //METADATA_H