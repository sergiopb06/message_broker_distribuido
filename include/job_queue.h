#ifndef JOB_QUEUE_H
#define JOB_QUEUE_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>

// Bounded FIFO of opaque pointers shared by producer and consumer threads.
// job_queue_push never blocks, so the MPI receiver can use it: when the queue is
// full or closed it fails and the caller answers "busy". job_queue_pop sleeps on
// a condition variable until there is an item or the queue is closed.
typedef struct {
    void **items;          // ring buffer of capacity slots
    size_t capacity;
    size_t head;           // index of the oldest item
    size_t count;
    bool closed;
    pthread_mutex_t mutex; // protects everything above
    pthread_cond_t not_empty;
} job_queue_t;

int job_queue_init(job_queue_t *queue, size_t capacity);

// Frees the ring buffer. Items still queued are not freed: drain them first.
void job_queue_destroy(job_queue_t *queue);

// 0 on success, -1 if the queue is full or closed.
int job_queue_push(job_queue_t *queue, void *item);

// Blocks until an item is available. Returns NULL once the queue is closed and
// empty, so every consumer can drain what was queued before closing.
void *job_queue_pop(job_queue_t *queue);

// No more pushes; wakes every consumer blocked in job_queue_pop.
void job_queue_close(job_queue_t *queue);

size_t job_queue_size(job_queue_t *queue);

#endif // JOB_QUEUE_H
