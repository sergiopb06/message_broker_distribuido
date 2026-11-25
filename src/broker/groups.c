#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>

#include "groups.h"
#include "common.h"
#include "protocol.h"
#include "metadata.h"
#include "log.h"
#include "mpi_layer.h"
#include "replication.h"
#include "job_queue.h"

#define MAX_GROUPS 64
#define MAX_MEMBERS 32
#define MAX_PARTITIONS 64
#define NO_OWNER 0
#define NO_OFFSET (-1)
#define REBALANCE_TIMEOUT_SECONDS 10
#define MAX_POLL_WAIT_MS 5000 //below REBALANCE_TIMEOUT_SECONDS, so a POLL never makes its member late
#define GONE_QUEUE_CAPACITY 1024


typedef enum{
    GROUP_EMPTY,
    GROUP_REBALANCING,
    GROUP_STABLE
} group_state_t;

typedef struct{
    unsigned long id; 
    long generation;
    int from_beginning;
    int next_poll;
    int joining; //waiting inside JOIN 
} member_t;


//One consumer group: the members of a topic that share its partitions
typedef struct{
    int used;
    char topic[TOPIC_NAME_MAX];
    char name[GROUP_NAME_MAX];
    int partitions;
    group_state_t state;
    long generation;
    struct timespec rebalance_deadline;

    member_t members[MAX_MEMBERS];
    int member_count;

    unsigned long owner[MAX_PARTITIONS]; //member that owns each partition
    long committed[MAX_PARTITIONS]; //next offset to read in each partition
    long position[MAX_PARTITIONS]; //next offset POLL returns

    pthread_mutex_t mutex;
    pthread_cond_t changed;                 
} group_t;


static group_t g_groups[MAX_GROUPS];
static pthread_mutex_t g_groups_mutex = PTHREAD_MUTEX_INITIALIZER;
static char g_data_dir[256];
static int g_rank;
static int g_size;
static atomic_int g_stopping;
static FILE *g_commit_file;
static pthread_mutex_t g_commit_mutex = PTHREAD_MUTEX_INITIALIZER;

//GROUP_GONE arrives on the MPI receiver thread, but removing a member needs the
//group mutexes, which other threads may hold across an MPI request. The receiver
//only queues the id and this worker does the removal, so it never blocks.
static job_queue_t g_gone_queue;
static pthread_t g_gone_thread;
static int g_gone_ready;

static uint32_t group_fnv1a(const char *data, size_t len)
{
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < len; i++) {
        hash ^= (unsigned char)data[i];
        hash *= 16777619u;
    }
    return hash;
}

static int group_coordinator(const char *name)
{
    if (g_size <= 1) {
        return 0;
    }
    return (int)(group_fnv1a(name, strlen(name)) % (uint32_t)g_size);
}




//HELPERS

static group_t *find_group(const char *topic, const char *name){
    for(int i=0; i < MAX_GROUPS; i++){
        if(g_groups[i].used && strcmp(g_groups[i].topic, topic) == 0 && strcmp(g_groups[i].name, name) == 0)
            return &g_groups[i];
        
    }
    return NULL;
}


static group_t *create_group(const char *topic, const char *name){
    for(int i=0; i < MAX_GROUPS; i++){
        if(!g_groups[i].used){
            group_t *group = &g_groups[i];

            memset(group, 0, sizeof *group);
            group->used = 1;
            snprintf(group->topic, sizeof group->topic, "%s", topic);
            snprintf(group->name, sizeof group->name, "%s", name);
            group->state = GROUP_EMPTY;


            for(int partition = 0; partition < MAX_PARTITIONS; partition++){
                group->owner[partition] = NO_OWNER;
                group->committed[partition] = NO_OFFSET;
                group->position[partition] = NO_OFFSET;
            }
            pthread_mutex_init(&group->mutex, NULL);
            pthread_cond_init(&group->changed, NULL);
            return group;
        }
    }
    return NULL;
}


static group_t *get_group(const char *topic, const char *name, int create){
    pthread_mutex_lock(&g_groups_mutex);
    group_t *group = find_group(topic, name);

    if(group == NULL && create){
        group = create_group(topic, name);
    }

    pthread_mutex_unlock(&g_groups_mutex);
    return group;
}


static unsigned long member_id(unsigned long connection){
    return ((unsigned long)g_rank << 32) | connection;
}

//Index of the member in the group
static int find_member(const group_t *group, unsigned long member){
    for(int i=0; i < group->member_count; i++){
        if(group->members[i].id == member)
            return i;
    }

    return -1;
}


static void remove_member_at(group_t *group, int index){
    for(int i = index; i < group->member_count - 1; i++){
        group->members[i] = group->members[i + 1];
    }

    group->member_count--;
}





static void start_rebalance(group_t *group){
    group->generation++;
    group->state = GROUP_REBALANCING;

    for(int partition = 0; partition < group->partitions; partition++)
        group->owner[partition] = NO_OWNER;

    clock_gettime(CLOCK_REALTIME, &group->rebalance_deadline);
    group->rebalance_deadline.tv_sec += REBALANCE_TIMEOUT_SECONDS;

    pthread_cond_broadcast(&group->changed);
    
}


static void try_finish_rebalance(group_t *group){
    if(group->state != GROUP_REBALANCING)
        return;


    if(group->member_count == 0){
        group->state = GROUP_EMPTY;
        return;
    }

    for(int i = 0; i < group->member_count; i++){
        if(group->members[i].generation != group->generation)
            return;
    }


    for(int partition = 0; partition < group->partitions; partition++){
        group->owner[partition] = group->members[partition % group->member_count].id;
        group->position[partition] = NO_OFFSET; //new owner starts from last commit
    }

    group->state = GROUP_STABLE;
    pthread_cond_broadcast(&group->changed);
}



static void remove_late_members(group_t *group){
    int removed = 0;

    for(int i = group->member_count - 1; i >= 0; i--){
        if(!group->members[i].joining && group->members[i].generation != group->generation){
            remove_member_at(group, i);
            removed = 1; //flag
        }
    }

    if(removed)
        start_rebalance(group);
}



static void remove_member(group_t *group, unsigned long id){
    int index = find_member(group, id);
    if(index < 0)
        return;

    remove_member_at(group, index);
    start_rebalance(group);
    try_finish_rebalance(group); //if it was the last member, the group becomes empty
}


static int save_commit(const group_t *group, int partition, long offset){
    int result = 0;

    pthread_mutex_lock(&g_commit_mutex);

    if(g_commit_file == NULL || fprintf(g_commit_file, "%s %s %d %ld\n", group->topic, group->name, partition, offset) < 0 || fflush(g_commit_file) != 0)
        result = -1;

    pthread_mutex_unlock(&g_commit_mutex);
    return result;
}




//Groups reloaded from commits.log do not know their topic yet: ask the metadata
static void ensure_partitions(group_t *group){
    if(group->partitions > 0)
        return;

    topic_info_t info;
    if(meta_topic_info(group->topic, &info) == MB_OK && info.partitions <= MAX_PARTITIONS)
        group->partitions = info.partitions;
}


static void write_assignment(const group_t *group, unsigned long id, char *out, size_t size){
    size_t used = (size_t)snprintf(out, size, "%ld ", group->generation);
    int any = 0;

    for(int partition = 0; partition < group->partitions && used < size; partition++){
        if(group->owner[partition] == id){
            int written;

            if(any)
                written = snprintf(out + used, size - used, ",%d", partition);

            else
                written = snprintf(out + used, size - used, "%d", partition);

            used += (size_t)written;
            any = 1;
        }
    }

    if(!any && used < size)
        snprintf(out + used, size - used, "-");
}


//POLL body: "<partition> <count>\n", then "<offset> <key_len> <value_len>\n<key><value>" per message
static int encode_messages(int partition, const message_t *msgs, size_t n, char **body, size_t *len){
    size_t size = 32;


    for(size_t i = 0; i < n; i++)
        size += 64 + msgs[i].key_len + msgs[i].value_len;


    char *out = malloc(size);
    if(out == NULL)
        return -1;


    size_t used = (size_t)snprintf(out, size, "%d %zu\n", partition, n);

    for(size_t i = 0; i < n; i++){
        used += (size_t)snprintf(out + used, size - used, "%ld %u %u\n", (long)msgs[i].offset, (unsigned)msgs[i].key_len, (unsigned)msgs[i].value_len);

        if(msgs[i].key_len > 0){
            memcpy(out + used, msgs[i].key, msgs[i].key_len);
            used += msgs[i].key_len;
        }


        memcpy(out + used, msgs[i].value, msgs[i].value_len);
        used += msgs[i].value_len;
    }


    *body = out;
    *len = used;
    return 0;
}



static void append_text(char *out, size_t size, size_t *used, const char *format, ...){
    if(*used >= size)
        return;

    va_list args;
    va_start(args, format);
    int written = vsnprintf(out + *used, size - *used, format, args);
    va_end(args);


    if(written > 0)
        *used += (size_t)written;
}


static const char *state_text(group_state_t state){
    if(state == GROUP_STABLE)
        return "stable";

    if(state == GROUP_REBALANCING)
        return "rebalancing";

    return "empty";
}



/////////////////////////////////////////////////////////////////////////////////////////////////////////////

static void handle_group_gone(int source, const void *buf, size_t len, void *arg);
static void *gone_worker(void *arg);

int groups_init(const char *data_dir, int rank, int size){
   snprintf(g_data_dir, sizeof g_data_dir, "%s", data_dir);
   g_rank = rank;
   g_size = size;
   atomic_store(&g_stopping, 0);

   // A client that disconnects without LEAVE is removed everywhere.
   if(job_queue_init(&g_gone_queue, GONE_QUEUE_CAPACITY) != 0)
       return -1;
   if(pthread_create(&g_gone_thread, NULL, gone_worker, NULL) != 0){
       job_queue_destroy(&g_gone_queue);
       return -1;
   }
   g_gone_ready = 1;
   mpil_register(MPIL_TAG_GROUP_GONE, handle_group_gone, NULL);

   char path[512];
   snprintf(path, sizeof path, "%s/commits.log", g_data_dir);


   FILE *file = fopen(path, "r");
   if(file != NULL){
       char topic[TOPIC_NAME_MAX];
       char name[GROUP_NAME_MAX];
       int partition;
       long offset;

       while(fscanf(file, "%63s %63s %d %ld", topic, name, &partition, &offset) == 4){
           if(partition < 0 || partition >= MAX_PARTITIONS)
               continue;

            group_t *group = get_group(topic, name, 1);
            if(group != NULL)
                group->committed[partition] = offset;
           
       }

       fclose(file);
   }

   g_commit_file = fopen(path, "a");
   if(g_commit_file == NULL){
        perror("fopen commits.log");
        return -1;
    }  
    
    return 0;
}



// `id` is already the cluster-wide id: (origin rank << 32) | connection.
// Forwarded requests carry that id, so members from different brokers never collide.
static int group_join_id(const char *topic, const char *name, unsigned long id, int from_beginning, char *out, size_t size);

int group_join(const char *topic, const char *name, unsigned long member, int from_beginning, char *out, size_t size){
    int coord = group_coordinator(name);
    unsigned long id = member_id(member);

    if(coord != g_rank){
        char req[256];
        snprintf(req, sizeof(req), "JOIN %s %s %lu %d", topic, name, id, from_beginning);
        void *reply = NULL;
        size_t reply_len = 0;
        int rc = mpil_request(coord, MPIL_TAG_FORWARD_REQ, req, strlen(req) + 1, &reply, &reply_len, 5000);
        if(rc != MB_OK){
            free(reply);
            return rc;
        }
        int status = -1;
        char body[2048] = {0};
        if(reply != NULL && reply_len > 0)
            (void)sscanf((const char *)reply, "%d %[^\n]", &status, body);
        if(out != NULL && size > 0)
            snprintf(out, size, "%s", body);
        free(reply);
        return status;
    }

    return group_join_id(topic, name, id, from_beginning, out, size);
}

static int group_join_id(const char *topic, const char *name, unsigned long id, int from_beginning, char *out, size_t size){
    topic_info_t info;
    int status = meta_topic_info(topic, &info);
    if(status != MB_OK)
        return status;

    if(info.partitions > MAX_PARTITIONS)
        return -1;


    group_t *group = get_group(topic, name, 1);
    if(group == NULL)
        return -1;

    pthread_mutex_lock(&group->mutex);

    if(group->partitions == 0)
        group->partitions = info.partitions;

    
    //A new member changes the group: start a rebalance
    int index = find_member(group, id);
    if(index < 0){
        if(group->member_count == MAX_MEMBERS){
            pthread_mutex_unlock(&group->mutex);
            return -1;
        }

        index = group->member_count;
        group->members[index].id = id;
        group->members[index].generation = -1;
        group->members[index].next_poll = 0;
        group->member_count++;
        start_rebalance(group);
    }

    group->members[index].from_beginning = from_beginning;
    group->members[index].joining = 1;


    //Accept the current generation and wait until every member did the same
    while(1){
        if(atomic_load(&g_stopping)){
            status = MB_ERR_SHUTTING_DOWN;
            break;
        }

        //The member may have been removed while waiting (its client disconnected
        //or it left): there is nothing left to assign
        index = find_member(group, id);
        if(index < 0){
            status = MB_ERR_REBALANCE;
            break;
        }
        group->members[index].generation = group->generation;
        try_finish_rebalance(group);

        if(group->state == GROUP_STABLE)
            break;


        if(pthread_cond_timedwait(&group->changed, &group->mutex, &group->rebalance_deadline) == ETIMEDOUT)
            remove_late_members(group);
    }

    index = find_member(group, id);
    if(index >= 0)
        group->members[index].joining = 0;

    if(status == MB_OK)
        write_assignment(group, id, out, size);

    pthread_mutex_unlock(&group->mutex);
    return status;
}




static int group_leave_id(const char *topic, const char *name, unsigned long id){
    group_t *group = get_group(topic, name, 0);
    if(group == NULL)
        return MB_OK;

    pthread_mutex_lock(&group->mutex);
    remove_member(group, id);
    pthread_mutex_unlock(&group->mutex);

    return MB_OK;
}

int group_leave(const char *topic, const char *name, unsigned long member){
    int coord = group_coordinator(name);
    unsigned long id = member_id(member);

    if(coord != g_rank){
        char req[256];
        snprintf(req, sizeof(req), "LEAVE %s %s %lu", topic, name, id);
        void *reply = NULL;
        size_t reply_len = 0;
        int rc = mpil_request(coord, MPIL_TAG_FORWARD_REQ, req, strlen(req) + 1, &reply, &reply_len, 5000);
        if(rc != MB_OK){
            free(reply);
            return rc;
        }
        int status = -1;
        if(reply != NULL && reply_len >= sizeof(int)){
            memcpy(&status, reply, sizeof(int));
        }
        free(reply);
        return status;
    }

    return group_leave_id(topic, name, id);
}

static void remove_member_from_all_groups(unsigned long id){
    pthread_mutex_lock(&g_groups_mutex);

    for(int i = 0; i < MAX_GROUPS; i++){
        if(!g_groups[i].used)
            continue;

        pthread_mutex_lock(&g_groups[i].mutex);
        remove_member(&g_groups[i], id);
        pthread_mutex_unlock(&g_groups[i].mutex);
    }

    pthread_mutex_unlock(&g_groups_mutex);
}

static void handle_group_gone(int source, const void *buf, size_t len, void *arg){
    (void)source;
    (void)arg;

    if(len < sizeof(unsigned long))
        return;

    unsigned long *id = malloc(sizeof *id);
    if(id == NULL)
        return;

    memcpy(id, buf, sizeof *id);
    //Full or closed queue: the member is dropped anyway when the next
    //rebalance times out without it
    if(job_queue_push(&g_gone_queue, id) != 0)
        free(id);
}

static void *gone_worker(void *arg){
    (void)arg;

    for(;;){
        unsigned long *id = job_queue_pop(&g_gone_queue);
        if(id == NULL)
            break;

        remove_member_from_all_groups(*id);
        free(id);
    }

    return NULL;
}

void group_member_gone(unsigned long member){
    unsigned long id = member_id(member);

    if(g_size > 1){
        for(int r = 0; r < g_size; r++){
            if(r != g_rank)
                mpil_send(r, MPIL_TAG_GROUP_GONE, &id, sizeof id);
        }
    }

    remove_member_from_all_groups(id);
}




//Commits are still accepted while the broker drains its in-flight requests:
//groups_close() makes them durable once every handler has finished
static int group_commit_id(const char *topic, const char *name, unsigned long id, long generation, int partition, long offset){
    if(offset < 0)
        return MB_ERR_OFFSET_OUT_OF_RANGE;

    group_t *group = get_group(topic, name, 0);
    if(group == NULL)
        return MB_ERR_REBALANCE;

    int status = MB_OK;

    pthread_mutex_lock(&group->mutex);

    //Only the owner of the partition in the current generation can commit
    if(group->state != GROUP_STABLE || generation != group->generation || partition < 0 || partition >= group->partitions || group->owner[partition] != id)
        status = MB_ERR_REBALANCE;


    else if(save_commit(group, partition, offset) < 0)
        status = -1;

    else
        group->committed[partition] = offset;


    pthread_mutex_unlock(&group->mutex);
    return status;
}

int group_commit(const char *topic, const char *name, unsigned long member, long generation, int partition, long offset){
    int coord = group_coordinator(name);
    unsigned long id = member_id(member);

    if(coord != g_rank){
        char req[256];
        snprintf(req, sizeof(req), "COMMIT %s %s %lu %ld %d %ld", topic, name, id, generation, partition, offset);
        void *reply = NULL;
        size_t reply_len = 0;
        int rc = mpil_request(coord, MPIL_TAG_FORWARD_REQ, req, strlen(req) + 1, &reply, &reply_len, 5000);
        if(rc != MB_OK){
            free(reply);
            return rc;
        }
        int status = -1;
        if(reply != NULL && reply_len >= sizeof(int)){
            memcpy(&status, reply, sizeof(int));
        }
        free(reply);
        return status;
    }

    return group_commit_id(topic, name, id, generation, partition, offset);
}




static int group_poll_id(const char *topic, const char *name, unsigned long id, size_t max_bytes, int wait_ms, char **body, size_t *len);

int group_poll(const char *topic, const char *name, unsigned long member, size_t max_bytes, int wait_ms, char **body, size_t *len){
    int coord = group_coordinator(name);
    unsigned long id = member_id(member);

    if(coord != g_rank){
        char req[256];
        snprintf(req, sizeof(req), "POLL %s %s %lu %zu %d", topic, name, id, max_bytes, wait_ms);
        void *reply = NULL;
        size_t reply_len = 0;
        int rc = mpil_request(coord, MPIL_TAG_FORWARD_REQ, req, strlen(req) + 1, &reply, &reply_len, wait_ms + 2000);
        if(rc != MB_OK){
            free(reply);
            return rc;
        }
        if(reply == NULL || reply_len < sizeof(int) + sizeof(uint32_t)){
            free(reply);
            return -1;
        }
        int status = 0;
        uint32_t body_len = 0;
        memcpy(&status, reply, sizeof(int));
        memcpy(&body_len, (const char *)reply + sizeof(int), sizeof(uint32_t));
        if(body_len > 0 && reply_len >= sizeof(int) + sizeof(uint32_t) + body_len){
            *body = malloc(body_len);
            if(*body != NULL){
                memcpy(*body, (const char *)reply + sizeof(int) + sizeof(uint32_t), body_len);
                *len = body_len;
            }
        }
        free(reply);
        return status;
    }

    return group_poll_id(topic, name, id, max_bytes, wait_ms, body, len);
}

static int group_poll_id(const char *topic, const char *name, unsigned long id, size_t max_bytes, int wait_ms, char **body, size_t *len){
    if(atomic_load(&g_stopping))
        return MB_ERR_SHUTTING_DOWN;


    *body = NULL;
    *len = 0;

    if(wait_ms < 0)
        wait_ms = 0;

    if(wait_ms > MAX_POLL_WAIT_MS)
        wait_ms = MAX_POLL_WAIT_MS;

    group_t *group = get_group(topic, name, 0);

    if(group == NULL)
        return MB_ERR_REBALANCE;   


    pthread_mutex_lock(&group->mutex);



    //1. Only a member of the current generation, with the group stable, can read
    int index = find_member(group, id);
    if(index < 0 || group->state != GROUP_STABLE || group->members[index].generation != group->generation){
        pthread_mutex_unlock(&group->mutex);
        return MB_ERR_REBALANCE;
    }



    long generation = group->generation;




    //2. Choose one of its partitions, starting after the last one it read.
    //   The log may live on another broker; repl_partition_offsets and
    //   repl_read take care of asking its leader over MPI.
    int chosen = -1;
    int has_data = 0;
    long from = 0;

    for(int step = 0; step < group->partitions && !has_data; step++){
        int partition = (group->members[index].next_poll + step) % group->partitions;

        if(group->owner[partition] != id)
            continue;

        int64_t start = 0;
        int64_t hwm = 0;
        int64_t leo = 0;
        if(repl_partition_offsets(topic, partition, &start, &hwm, &leo) != MB_OK)
            continue;

        if(group->position[partition] == NO_OFFSET){
            if(group->committed[partition] != NO_OFFSET)
                group->position[partition] = group->committed[partition];

            else if(group->members[index].from_beginning)
                group->position[partition] = (long)start;

            else
                group->position[partition] = (long)hwm;
        }

        int ready = hwm > group->position[partition];

        if(chosen < 0 || ready){
            chosen = partition;
            from = group->position[partition];
            has_data = ready;
        }
    }


    //A member without partitions sleeps until wait_ms or a rebalance
    if(chosen < 0){
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += wait_ms / 1000;
        deadline.tv_nsec += (long)(wait_ms % 1000) * 1000000L;


        if(deadline.tv_nsec >= 1000000000L){
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }



        while(group->generation == generation && !atomic_load(&g_stopping)){

            if(pthread_cond_timedwait(&group->changed, &group->mutex, &deadline) == ETIMEDOUT)
                break;
        }

        int status = MB_OK;

        if(atomic_load(&g_stopping))
            status = MB_ERR_SHUTTING_DOWN;


        else if(group->generation != generation)
            status = MB_ERR_REBALANCE;


        pthread_mutex_unlock(&group->mutex);


        if(status == MB_OK && encode_messages(-1, NULL, 0, body, len) != 0)
            status = -1;

        return status;
    }


    group->members[index].next_poll = chosen + 1;
    pthread_mutex_unlock(&group->mutex);



    //3. Wait and read without the group mutex
    if(!has_data){
        int waited = repl_wait_data(topic, chosen, from, wait_ms);

        if(waited == MB_ERR_TIMEOUT){
            if(encode_messages(chosen, NULL, 0, body, len) != 0)
                return -1;
            return MB_OK;   //nothing new is not an error
        }

        if(waited != MB_OK)
            return waited;
    }

    message_t *msgs = NULL;
    size_t n = 0;
    int status = repl_read(topic, chosen, from, max_bytes, &msgs, &n);


    if(status != MB_OK)
        return status;




    //4. Deliver only if it still owns the partition
    pthread_mutex_lock(&group->mutex);

    if(group->generation != generation || group->owner[chosen] != id)
        status = MB_ERR_REBALANCE;

    else if(n > 0)
        group->position[chosen] = (long)msgs[n - 1].offset + 1;

    pthread_mutex_unlock(&group->mutex);




    //5. Build the reply
    if(status == MB_OK && encode_messages(chosen, msgs, n, body, len) != 0)
        status = -1;

    plog_free_messages(msgs);
    return status;
}



int group_describe(const char *name, char *out, size_t size){
    int coord = group_coordinator(name);
    if(coord != g_rank){
        char req[256];
        snprintf(req, sizeof(req), "DESCRIBE %s", name);
        void *reply = NULL;
        size_t reply_len = 0;
        int rc = mpil_request(coord, MPIL_TAG_FORWARD_REQ, req, strlen(req) + 1, &reply, &reply_len, 5000);
        if(rc != MB_OK){
            free(reply);
            return rc;
        }
        if(reply == NULL || reply_len < sizeof(int)){
            free(reply);
            return -1;
        }
        int status = -1;
        memcpy(&status, reply, sizeof(int));
        if(reply_len > sizeof(int))
            snprintf(out, size, "%.*s", (int)(reply_len - sizeof(int)), (const char *)reply + sizeof(int));
        free(reply);
        return status;
    }

    size_t used = 0;
    int found = 0;
    out[0] = '\0';

    pthread_mutex_lock(&g_groups_mutex);

    for(int i = 0; i < MAX_GROUPS; i++){
        group_t *group = &g_groups[i];

        if(!group->used || strcmp(group->name, name) != 0)
            continue;

        found = 1;
        pthread_mutex_lock(&group->mutex);
        ensure_partitions(group);

        append_text(out, size, &used, "group %s topic %s state %s generation %ld\n", group->name, group->topic, state_text(group->state), group->generation);

        for(int partition = 0; partition < group->partitions; partition++){
            append_text(out, size, &used, "  partition %d member ", partition);

            if(group->owner[partition] == NO_OWNER)
                append_text(out, size, &used, "-");

            else
                append_text(out, size, &used, "%lu:%lu", group->owner[partition] >> 32, group->owner[partition] & 0xFFFFFFFFUL);

            partition_log_t *log = meta_get_log(group->topic, partition);
            int64_t hwm = -1;
            if(log != NULL){
                hwm = plog_hwm(log);
            } else {
                int64_t leo = -1;
                (void)repl_partition_state(group->topic, partition, NULL, NULL, &hwm, &leo);
            }

            if(group->committed[partition] == NO_OFFSET || hwm < 0)
                append_text(out, size, &used, " committed - lag -\n");

            else
                append_text(out, size, &used, " committed %ld lag %ld\n", group->committed[partition], (long)hwm - group->committed[partition]);
        }

        pthread_mutex_unlock(&group->mutex);
    }

    pthread_mutex_unlock(&g_groups_mutex);

    if(!found)
        append_text(out, size, &used, "group %s not found\n", name);

    return MB_OK;
}

void groups_handle_forward(int source, uint64_t req_id, const char *line, size_t len)
{
    (void)len;
    // The member id in a forwarded line is already global, so call the local
    // implementations directly: re-encoding it here would prefix this rank.
    if (strncmp(line, "JOIN ", 5) == 0) {
        char topic[TOPIC_NAME_MAX], name[GROUP_NAME_MAX];
        unsigned long member = 0;
        int from_beginning = 0;
        if (sscanf(line + 5, "%s %s %lu %d", topic, name, &member, &from_beginning) == 4) {
            char body[2048] = {0};
            int status = group_join_id(topic, name, member, from_beginning, body, sizeof(body));
            char reply[2200];
            snprintf(reply, sizeof(reply), "%d %s", status, body);
            mpil_reply(source, req_id, reply, strlen(reply) + 1);
            return;
        }
    } else if (strncmp(line, "LEAVE ", 6) == 0) {
        char topic[TOPIC_NAME_MAX], name[GROUP_NAME_MAX];
        unsigned long member = 0;
        if (sscanf(line + 6, "%s %s %lu", topic, name, &member) == 3) {
            int status = group_leave_id(topic, name, member);
            mpil_reply(source, req_id, &status, sizeof(status));
            return;
        }
    } else if (strncmp(line, "COMMIT ", 7) == 0) {
        char topic[TOPIC_NAME_MAX], name[GROUP_NAME_MAX];
        unsigned long member = 0;
        long generation = 0, offset = 0;
        int partition = 0;
        if (sscanf(line + 7, "%s %s %lu %ld %d %ld", topic, name, &member, &generation, &partition, &offset) == 6) {
            int status = group_commit_id(topic, name, member, generation, partition, offset);
            mpil_reply(source, req_id, &status, sizeof(status));
            return;
        }
    } else if (strncmp(line, "POLL ", 5) == 0) {
        char topic[TOPIC_NAME_MAX], name[GROUP_NAME_MAX];
        unsigned long member = 0;
        size_t max_bytes = 0;
        int wait_ms = 0;
        if (sscanf(line + 5, "%s %s %lu %zu %d", topic, name, &member, &max_bytes, &wait_ms) == 5) {
            char *body = NULL;
            size_t body_len = 0;
            int status = group_poll_id(topic, name, member, max_bytes, wait_ms, &body, &body_len);
            size_t reply_len = sizeof(int) + sizeof(uint32_t) + body_len;
            char *reply = malloc(reply_len);
            if (reply != NULL) {
                memcpy(reply, &status, sizeof(int));
                uint32_t ulen = (uint32_t)body_len;
                memcpy(reply + sizeof(int), &ulen, sizeof(uint32_t));
                if (body_len > 0 && body != NULL) {
                    memcpy(reply + sizeof(int) + sizeof(uint32_t), body, body_len);
                }
                mpil_reply(source, req_id, reply, reply_len);
                free(reply);
            }
            free(body);
            return;
        }
    } else if (strncmp(line, "DESCRIBE ", 9) == 0) {
        char name[GROUP_NAME_MAX];
        if (sscanf(line + 9, "%s", name) == 1) {
            char body[4096] = {0};
            int status = group_describe(name, body, sizeof(body));
            size_t body_len = strlen(body) + 1;
            size_t reply_len = sizeof(int) + body_len;
            char *reply = malloc(reply_len);
            if (reply != NULL) {
                memcpy(reply, &status, sizeof(int));
                memcpy(reply + sizeof(int), body, body_len);
                mpil_reply(source, req_id, reply, reply_len);
                free(reply);
            }
            return;
        }
    }

    int err = -1;
    mpil_reply(source, req_id, &err, sizeof(err));
}



//Phase 2 of the shutdown: wake every JOIN and POLL waiting on a group, so the
//connection handlers can finish

void groups_shutdown(void){
    atomic_store(&g_stopping, 1);

    pthread_mutex_lock(&g_groups_mutex);

    for(int i = 0; i < MAX_GROUPS; i++){
        if(!g_groups[i].used)
            continue;



        pthread_mutex_lock(&g_groups[i].mutex);
        pthread_cond_broadcast(&g_groups[i].changed);
        pthread_mutex_unlock(&g_groups[i].mutex);
    }

    pthread_mutex_unlock(&g_groups_mutex);
}


//Phase 3, once no handler is left: stop the GROUP_GONE worker and make the
//committed offsets durable

void groups_close(void){
    atomic_store(&g_stopping, 1);

    if(g_gone_ready){
        job_queue_close(&g_gone_queue);
        pthread_join(g_gone_thread, NULL);
        for(unsigned long *id; (id = job_queue_pop(&g_gone_queue)) != NULL;)
            free(id);
        job_queue_destroy(&g_gone_queue);
        g_gone_ready = 0;
    }

    pthread_mutex_lock(&g_commit_mutex);


    if(g_commit_file != NULL){
        fflush(g_commit_file);
        fsync(fileno(g_commit_file));
        fclose(g_commit_file);
        g_commit_file = NULL;
    }

    pthread_mutex_unlock(&g_commit_mutex);
}