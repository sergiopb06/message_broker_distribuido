#ifndef GROUPS_H
#define GROUPS_H

#include <stddef.h>
#include <stdint.h>

//member is the id of the client connection that joined the group

int groups_init(const char *data_dir, int rank, int size);

void groups_shutdown(void);

int group_join(const char *topic, const char *group, unsigned long member, int from_beginning, char *out, size_t size);

int group_poll(const char *topic, const char *group, unsigned long member, size_t max_bytes, int wait_ms, char **body, size_t *len);

//Saves the next offset to read of one partition.
int group_commit(const char *topic, const char *group, unsigned long member, long generation, int partition, long offset);

int group_leave(const char *topic, const char *group, unsigned long member);

void group_member_gone(unsigned long member);

//Text with the members, their partitions, the last committed offset and the lag.
int group_describe(const char *group, char *out, size_t size);

void groups_handle_forward(int source, uint64_t req_id, const char *line, size_t len);

#endif //GROUPS_H