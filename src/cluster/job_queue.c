#define _POSIX_C_SOURCE 200809L

#include "job_queue.h"

#include <stdlib.h>

int job_queue_init(job_queue_t *queue, size_t capacity)
{
    if (queue == NULL || capacity == 0) {
        return -1;
    }
    queue->items = calloc(capacity, sizeof *queue->items);
    if (queue->items == NULL) {
        return -1;
    }
    queue->capacity = capacity;
    queue->head = 0;
    queue->count = 0;
    queue->closed = false;
    if (pthread_mutex_init(&queue->mutex, NULL) != 0) {
        free(queue->items);
        return -1;
    }
    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&queue->mutex);
        free(queue->items);
        return -1;
    }
    return 0;
}

void job_queue_destroy(job_queue_t *queue)
{
    if (queue == NULL || queue->items == NULL) {
        return;
    }
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->mutex);
    free(queue->items);
    queue->items = NULL;
}

int job_queue_push(job_queue_t *queue, void *item)
{
    pthread_mutex_lock(&queue->mutex);
    if (queue->closed || queue->count == queue->capacity) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }
    queue->items[(queue->head + queue->count) % queue->capacity] = item;
    queue->count++;
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
    return 0;
}

void *job_queue_pop(job_queue_t *queue)
{
    pthread_mutex_lock(&queue->mutex);
    while (queue->count == 0 && !queue->closed) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }
    void *item = NULL;
    if (queue->count > 0) {
        item = queue->items[queue->head];
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;
    }
    pthread_mutex_unlock(&queue->mutex);
    return item;
}

void job_queue_close(job_queue_t *queue)
{
    pthread_mutex_lock(&queue->mutex);
    queue->closed = true;
    pthread_cond_broadcast(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
}

size_t job_queue_size(job_queue_t *queue)
{
    pthread_mutex_lock(&queue->mutex);
    size_t count = queue->count;
    pthread_mutex_unlock(&queue->mutex);
    return count;
}
