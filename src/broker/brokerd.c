#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "net_util.h"
#include "protocol.h"
#include "common.h"
#include "metadata.h"
#include "mpi_layer.h"
#include "replication.h"
#include "groups.h"

#include <stdatomic.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <semaphore.h>
#include <limits.h>
#include <sys/time.h>
#include <stdint.h>
#include <netinet/in.h>
#include <netinet/tcp.h>


#define DEFAULT_PORT 9000
#define LISTEN_BACKLOG 64


#define REPLY_BYTES 2048 //first size of the reply buffer; it doubles while the reply doesn't fit

#define IDLE_TIMEOUT_SECONDS 60

#ifndef MAX_ACTIVE_THREADS
#define MAX_ACTIVE_THREADS 256
#endif

static volatile sig_atomic_t g_running = 1;
static int listen_file_descriptor = -1;
static int g_rank = 0;

// Limits how many connection handlers can run at once.
static sem_t g_counting_sem;


typedef struct {
    int file_descriptor;
    unsigned long connection_id;
    int slot; //g_connections
} connection_t;


//Descriptors of the active connections (-1 = free)
static int g_connections[MAX_ACTIVE_THREADS];
static pthread_mutex_t g_connections_mutex = PTHREAD_MUTEX_INITIALIZER;

int server_is_running(void);
void server_request_shutdown(void);

// Only async-signal-safe calls here; main does the rest after the accept loop.
static void on_sigint(int signum)
{
    (void)signum;
    g_running = 0;
    if(listen_file_descriptor >= 0)
        shutdown(listen_file_descriptor, SHUT_RD);
}

int server_is_running(void){
    return g_running;
}

void server_request_shutdown(void){
    g_running = 0;
    // A paused broker would otherwise keep every connection thread asleep and
    // the cluster would never finish the ordered shutdown.
    repl_set_paused(false);
    if(listen_file_descriptor >= 0)
        shutdown(listen_file_descriptor, SHUT_RD);
}

static int install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;

    if (sigaction(SIGINT, &sa, NULL) < 0)
    {
        perror("sigaction");
        return -1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;

    if (sigaction(SIGPIPE, &sa, NULL) < 0)
    {
        perror("sigaction");
        return -1;
    }

    return 0;
}


static void sem_wait_retry(sem_t *s){
    while (sem_wait(s) < 0 && errno == EINTR){}
}



static int register_connection(int file_descriptor){

    int slot = -1;

    pthread_mutex_lock(&g_connections_mutex);

    for(int i = 0; i < MAX_ACTIVE_THREADS && slot < 0; i++){
        if(g_connections[i] < 0){
            g_connections[i] = file_descriptor;
            slot = i;
        }
    }

    pthread_mutex_unlock(&g_connections_mutex);
    return slot;
}


static void unregister_connection(int slot){

    if(slot < 0)
        return;

    pthread_mutex_lock(&g_connections_mutex);
    g_connections[slot] = -1;
    pthread_mutex_unlock(&g_connections_mutex);
}



static void wake_connections(void){
    pthread_mutex_lock(&g_connections_mutex);

    for(int i = 0; i < MAX_ACTIVE_THREADS; i++){
        if(g_connections[i] >= 0)
            shutdown(g_connections[i], SHUT_RD);
    }

    pthread_mutex_unlock(&g_connections_mutex);
}




static const char *status_text(int status){

    if(status == MB_ERR_UNKNOWN_TOPIC)
        return "UNKNOWN_TOPIC";

    if(status == MB_ERR_TOPIC_EXISTS)
        return "TOPIC_EXISTS";

    if(status == MB_ERR_NOT_LEADER)
        return "NOT_LEADER";

    if(status == MB_ERR_OFFSET_OUT_OF_RANGE)
        return "OFFSET_OUT_OF_RANGE";

    if(status == MB_ERR_REBALANCE)
        return "REBALANCE";

    if(status == MB_ERR_TIMEOUT)
        return "TIMEOUT";

    if(status == MB_ERR_SHUTTING_DOWN)
        return "SHUTTING_DOWN";

    if(status == MB_ERR_INVALID_ARGUMENT)
        return "INVALID_ARGUMENT";

    return "ERROR";
}



//TOPIC LIST, TOPIC DESCRIBE and GROUP DESCRIBE: the text reply can be long.
//A reply that fills the buffer was probably cut
static void reply_text(int file_descriptor, const command_t *command){
    char *body = NULL;
    size_t size = REPLY_BYTES;
    int status;

    while(1){
        char *bigger = realloc(body, size);

        if(bigger == NULL){
            (void)protocol_send_response(file_descriptor, 0, "out of memory");
            free(body);
            return;
        }

        body = bigger;
        body[0] = '\0';

        if(command->type == C_TOPIC_LIST)
            status = meta_list(body, size);

        else if(command->type == C_GROUP_DESCRIBE)
            status = group_describe(command->group, body, size);

        else
            status = meta_describe(command->topic, body, size);

        if(status != MB_OK || strlen(body) < size - 1 || size >= MAX_RESPONSE_BYTES)
            break;

        size *= 2;
    }

    if(status == MB_OK)
        (void)protocol_send_response(file_descriptor, 1, body);

    else
        (void)protocol_send_response(file_descriptor, 0, status_text(status));



    free(body);
}


//---------------------------------------------------------------------------

//FNV-1a hash: http://www.isthe.com/chongo/tech/comp/fnv/

static atomic_uint g_next_partition = 0; //for messages without a key

//FNV-1a: the same bytes always give the same number
static uint32_t fnv1a(const char *data, size_t len){
    uint32_t hash = 2166136261u;

    for(size_t i = 0; i < len; i++){
        hash ^= (unsigned char)data[i];
        hash *= 16777619u;
    }

    return hash;
}


//Same key, same partition. Without a key it goes round-robin.
static int choose_partition(const char *key, size_t key_len, int partitions){

    if(key_len == 0)
        return (int)(atomic_fetch_add(&g_next_partition, 1) % (unsigned)partitions);

    return (int)(fnv1a(key, key_len) % (uint32_t)partitions);
}

//---------------------------------------------------------------------------


//Appends one message to a partition this broker leads and replies "<partition> <offset>".
static void produce_message(int file_descriptor, const command_t *command, const char *key, const char *value, int partition, int replication){

    partition_log_t *log = meta_get_log(command->topic, partition);

    if(log == NULL){
        (void)protocol_send_response(file_descriptor, 0, "ERROR");
        return;
    }


    message_t message;
    memset(&message, 0, sizeof message);
    message.key_len = (uint32_t)command->key_len;
    message.value_len = (uint32_t)command->value_len;
    message.value = (char *)value;

    if(command->key_len > 0)
        message.key = (char *)key;

    char reply[64];

    //acks 0
    if(command->acks == ACKS_NONE){
        snprintf(reply, sizeof reply, "%d -1", partition);
        (void)protocol_send_response(file_descriptor, 1, reply);
    }


    long offset;
    int status = plog_append(log, &message, 1, &offset);

    if(status == MB_OK && replication == 1)
        plog_set_hwm(log, offset + 1);

    if(status == MB_OK && replication > 1)
        repl_notify_append(command->topic, partition);

    if(command->acks == ACKS_NONE)
        return;

    if(status != MB_OK){
        (void)protocol_send_response(file_descriptor, 0, status_text(status));
        return;
    }

    if(command->acks == ACKS_ALL && replication > 1){
        status = repl_wait_acks(command->topic, partition, offset, command->acks, 5000);
        if(status != MB_OK){
            (void)protocol_send_response(file_descriptor, 0, status_text(status));
            return;
        }
    }

    snprintf(reply, sizeof reply, "%d %ld", partition, offset);
    (void)protocol_send_response(file_descriptor, 1, reply);
}



//Returns 0 to keep it open, -1 to close it
static int server_command(int file_descriptor, unsigned long member, const command_t *command){
    char *key = NULL;
    char *value = NULL;
    int result = 0;

    if(command->type == C_PRODUCE){
        key = malloc(command->key_len + 1);
        value = malloc(command->value_len + 1);

        if(key == NULL || value == NULL){
            (void)protocol_send_response(file_descriptor, 0, "out of memory");
            free(key);
            free(value);
            return -1;
        }


        if((command->key_len > 0 && nu_read_exact(file_descriptor, key, command->key_len) < 0) || nu_read_exact(file_descriptor, value, command->value_len) < 0){
            (void)protocol_send_response(file_descriptor, 0, "incomplete body");
            free(key);
            free(value);
            return -1;
        }

        key[command->key_len] = '\0';
        value[command->value_len] = '\0';
    }


    //TOPIC CREATE
    if(command->type == C_TOPIC_CREATE){
        int status = meta_create_topic(command->topic, command->partitions, command->replication, (uint64_t)command->retention_bytes);
        if(status == MB_ERR_NOT_LEADER && g_rank != 0){
            status = repl_forward_topic_create(command->topic, command->partitions, command->replication, (uint64_t)command->retention_bytes);
        } else if(status == MB_OK && g_rank == 0){
            repl_broadcast_metadata();
        }

        if(status == MB_OK)
            (void)protocol_send_response(file_descriptor, 1, "topic created");

        else
            (void)protocol_send_response(file_descriptor, 0, status_text(status));
    }

    //TOPIC LIST | TOPIC DESCRIBE | GROUP DESCRIBE
    else if(command->type == C_TOPIC_LIST || command->type == C_TOPIC_DESCRIBE || command->type == C_GROUP_DESCRIBE)
        reply_text(file_descriptor, command);

        

    

    //BROKER PAUSE | RESUME
    else if(command->type == C_BROKER_PAUSE || command->type == C_BROKER_RESUME){
        admin_cmd_t admin = ADMIN_RESUME;

        if(command->type == C_BROKER_PAUSE)
            admin = ADMIN_PAUSE;

        int status = cluster_admin(command->broker_id, admin);

        if(status == MB_OK)
            (void)protocol_send_response(file_descriptor, 1, "done");

        else
            (void)protocol_send_response(file_descriptor, 0, status_text(status));
    }

    //SHUTDOWN
    else if(command->type == C_SHUTDOWN){
        int status = cluster_admin(-1, ADMIN_SHUTDOWN); //-1: the whole cluster

        if(status == MB_OK){
            (void)protocol_send_response(file_descriptor, 1, "shutting down");
            server_request_shutdown();
            result = -1; //close this session
        }

        else
            (void)protocol_send_response(file_descriptor, 0, status_text(status));
    }




    //JOIN
    else if(command->type == C_JOIN){
        char body[REPLY_BYTES];
        int status = group_join(command->topic, command->group, member, command->from_beginning, body, sizeof body);

        if(status == MB_OK)
            (void)protocol_send_response(file_descriptor, 1, body);

        else
            (void)protocol_send_response(file_descriptor, 0, status_text(status));
    }



    //POLL
    else if(command->type == C_POLL){
        char *body = NULL;
        size_t len = 0;
        int status = group_poll(command->topic, command->group, member, command->max_bytes, command->wait_ms, &body, &len);

        if(status == MB_OK)
            (void)protocol_send_response_bytes(file_descriptor, 1, body, len);

        else
            (void)protocol_send_response(file_descriptor, 0, status_text(status));

        free(body); 
    }



    //COMMIT
    else if(command->type == C_COMMIT){
        int status = group_commit(command->topic, command->group, member, command->generation, command->partition, command->offset);

        if(status == MB_OK)
            (void)protocol_send_response(file_descriptor, 1, "committed");

        else
            (void)protocol_send_response(file_descriptor, 0, status_text(status));
    }



    //LEAVE
    else if(command->type == C_LEAVE){
        int status = group_leave(command->topic, command->group, member);

        if(status == MB_OK)
            (void)protocol_send_response(file_descriptor, 1, "left");

        else
            (void)protocol_send_response(file_descriptor, 0, status_text(status));
    }




    //PRODUCE
    else if(command->type == C_PRODUCE){

        topic_info_t info;
        partition_info_t where;
        int partition = -1;
        int status = meta_topic_info(command->topic, &info);

        if(status == MB_OK){
            partition = choose_partition(key, command->key_len, info.partitions);
            status = meta_get_partition(command->topic, partition, &where);
        }

        if(status != MB_OK)
            (void)protocol_send_response(file_descriptor, 0, status_text(status));


        else if(where.leader != g_rank){
            // The leader can be another broker: forward the append over MPI.
            long offset = -1;
            int forwarded = repl_forward_produce(command->topic, partition, key, command->key_len,
                                                 value, command->value_len, command->acks, &offset);

            if(forwarded != MB_OK){
                (void)protocol_send_response(file_descriptor, 0, status_text(forwarded));
            } else if(command->acks == ACKS_NONE){
                char reply[64];
                snprintf(reply, sizeof reply, "%d -1", partition);
                (void)protocol_send_response(file_descriptor, 1, reply);
            } else {
                char reply[64];
                snprintf(reply, sizeof reply, "%d %ld", partition, offset);
                (void)protocol_send_response(file_descriptor, 1, reply);
            }
        }


        else
            produce_message(file_descriptor, command, key, value, partition, info.replication);
    }


    free(key);
    free(value);
    return result;
}


static void *handle_connection(void *arg)
{
    connection_t *conn = arg;
    char line[LINE_MAX_LEN];

    //A client that connects and sends nothing can't keep this handler blocked forever
    struct timeval timeout = {IDLE_TIMEOUT_SECONDS, 0};
    (void)setsockopt(conn->file_descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);

    int on = 1;
    (void)setsockopt(conn->file_descriptor, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);

    while (g_running){
        errno = 0;
        ssize_t n = nu_read_line(conn->file_descriptor, line, sizeof line);

        if (n < 0){
            if(errno == EMSGSIZE)
                (void)protocol_send_response(conn->file_descriptor, 0, "line too long");

            else if(errno == EAGAIN || errno == EWOULDBLOCK)
                (void)protocol_send_response(conn->file_descriptor, 0, "idle timeout");

            break;//disconnected or error

        }


        if (n == 0){
            (void)protocol_send_response(conn->file_descriptor, 0, "empty line");
            continue;
        }

        command_t command;

        if (protocol_parse(line, &command) < 0){
            (void)protocol_send_response(conn->file_descriptor, 0, command.error);
            break;
        }

        // Paused broker: replication stops, but PAUSE, RESUME and SHUTDOWN must
        // still be accepted, otherwise the broker could never be resumed.
        if (repl_is_paused() && command.type != C_BROKER_PAUSE
            && command.type != C_BROKER_RESUME && command.type != C_SHUTDOWN)
            repl_wait_if_paused();

        if (!g_running)
            break;

        if(server_command(conn->file_descriptor, conn->connection_id, &command) < 0)
            break;

    }


    //A consumer that disconnects without LEAVE
    group_member_gone(conn->connection_id);

    unregister_connection(conn->slot);
        
    if (close(conn->file_descriptor) < 0)
        perror("close(file_descriptor)");


    free(conn);
    sem_post(&g_counting_sem);
    return NULL;

}

static int parse_int(const char *text, long min, long max, long *out)
{
    char *end = NULL;
    errno = 0;
    long value = strtol(text, &end, 10);

    if(errno != 0 || end == text || *end != '\0' || value < min || value > max)
        return -1;

    *out = value;
    return 0;
}


static int parse_args(int argc, char **argv, unsigned short *base_port, const char **data_directory){
    long value_port;

    *base_port = DEFAULT_PORT;
    *data_directory = "./data";

    for(int i = 1; i < argc; i++){
        if(strcmp(argv[i], "--base-port") == 0){

            if(i + 1 >= argc){
                fprintf(stderr, "missing value for --base-port\n");
                return -1;
            }

            if(parse_int(argv[i + 1], 1, 65535, &value_port) < 0){
                fprintf(stderr, "invalid --base-port '%s'\n", argv[i + 1]);
                return -1;
            }

            i++;
            *base_port = (unsigned short)value_port;
        }


        else if(strcmp(argv[i], "--data") == 0){

            if(i + 1 >= argc){
                fprintf(stderr, "missing value for --data\n");
                return -1;
            }

            i++;
            *data_directory = argv[i];

        }

        else{
            fprintf(stderr, "unknown argument '%s'\n", argv[i]);
            fprintf(stderr, "usage: %s [--base-port <port>] [--data <directory>]\n", argv[0]);
            return -1;
        }
    }

    return 0;
}



int main(int argc, char **argv)
{

    unsigned short base_port;
    const char *data_directory;
    int rank;
    int size;

    //MPI
    if(mpil_init(&argc, &argv, &rank, &size) < 0)
        return EXIT_FAILURE;

    g_rank = rank;

    if(parse_args(argc, argv, &base_port, &data_directory) < 0)
        return EXIT_FAILURE;


    long port = (long)base_port + rank;

    if(port > 65535){
        fprintf(stderr, "port %ld is out of range\n", port);
        return EXIT_FAILURE;
    }


    //Every broker keeps its data in its own directory: <data>/broker<rank>
    char broker_directory[PATH_MAX];
    int written = snprintf(broker_directory, sizeof broker_directory, "%s/broker%d", data_directory, rank);

    if(written < 0 || (size_t)written >= sizeof broker_directory){
        fprintf(stderr, "data directory path is too long\n");
        return EXIT_FAILURE;
    }

    //Creates the directory
    if(meta_init(rank, size, broker_directory) != 0){
        perror("meta_init");
        return EXIT_FAILURE;
    }


    //Loads the committed offsets of the groups
    if(groups_init(broker_directory, rank, size) != 0){
        meta_destroy();
        return EXIT_FAILURE;
    }

    if(repl_init(rank, size) != 0 || repl_start() != 0){
        groups_shutdown();
        groups_close();
        meta_destroy();
        return EXIT_FAILURE;
    }

    if(install_signal_handlers() < 0)
        return EXIT_FAILURE;


    for(int i = 0; i < MAX_ACTIVE_THREADS; i++)
        g_connections[i] = -1;


    if(sem_init(&g_counting_sem, 0, MAX_ACTIVE_THREADS) < 0){
        perror("sem_init");
        return EXIT_FAILURE;
    }

    listen_file_descriptor = nu_listen((unsigned short)port, LISTEN_BACKLOG);

    if (listen_file_descriptor < 0)
    {
        groups_shutdown();
        groups_close();
        meta_destroy();
        sem_destroy(&g_counting_sem);
        return EXIT_FAILURE;
    }

    printf("broker %d listening on port %ld — Ctrl-C to stop\n", rank, port);
    fflush(stdout);

    unsigned long accepted = 0; 


    while (g_running)
    {                        
         
        if (sem_wait(&g_counting_sem) < 0){
            
            if (errno == EINTR){
                continue;             
            }

            perror("sem_wait");
            break;
        }
        
        
        int client_file_descriptor = accept4(listen_file_descriptor, NULL, NULL, SOCK_CLOEXEC);
        if (client_file_descriptor < 0)
        {
            sem_post(&g_counting_sem);
            if (errno == EINTR)
            {
                continue;
            }

            if(!g_running)
            {
                break;
            }

            perror("accept");
            break;
        }
        
        connection_t *conn = malloc(sizeof(connection_t));

        if (conn == NULL)
        {
            fprintf(stderr, "out of memory, dropping connection\n");
            close(client_file_descriptor);
            sem_post(&g_counting_sem); 
            continue;
        }

        conn->file_descriptor = client_file_descriptor;
        conn->connection_id = ++accepted;
        conn->slot = register_connection(client_file_descriptor);

        pthread_t thread_id;

        int pthread_created = pthread_create(&thread_id, NULL, handle_connection, conn);

        if (pthread_created != 0)
        {
            fprintf(stderr, "pthread_create failed %s\n", strerror(pthread_created));
            unregister_connection(conn->slot);
            close(client_file_descriptor);
            free(conn);
            --accepted;
            sem_post(&g_counting_sem); 
            continue;
        }
        
        pthread_created = pthread_detach(thread_id);
        if (pthread_created != 0)
        {
            fprintf(stderr, "pthread_detach failed %s\n", strerror(pthread_created));
        }

    }

    // Also needed after SIGINT: wakes a paused broker.
    server_request_shutdown();

    if (close(listen_file_descriptor))
    {
        perror("close(listen_file_descriptor)");
    }
    listen_file_descriptor = -1;


    wake_connections();

    groups_shutdown(); 

    // No forwarded request can succeed once we are stopping: unblock every
    // handler that is waiting for an MPI reply before joining them.
    mpil_abort_requests();

    //wait for every handler to finish
    for(int i = 0; i < MAX_ACTIVE_THREADS; i++){
        sem_wait_retry(&g_counting_sem);
    }

    repl_shutdown();

    // No client handler, forward worker or MPI receiver is left: stop the
    // GROUP_GONE worker and make the committed offsets durable.
    groups_close();

    meta_destroy();

    sem_destroy(&g_counting_sem);

    printf("broker %d stopped (%lu connections accepted)\n", rank, accepted);

    return EXIT_SUCCESS;
}