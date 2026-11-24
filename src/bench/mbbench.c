#define _POSIX_C_SOURCE 200809L

#include "client.h"
#include "protocol.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define HOT_KEY "hot"
#define POLL_MAX_BYTES (1024 * 1024)
#define POLL_WAIT_MS 500
#define CONSUMER_IDLE_MS 2000


typedef struct{
    const char *host;
    unsigned short port;
    const char *topic;
    int producers;
    long messages; //per producer
    size_t message_bytes;
    acks_t acks;
    int consumers;
    const char *group;
    int hot_percent; //percentage of messages sent with the hot key
} bench_config_t;



typedef struct{
    const bench_config_t *config;
    pthread_barrier_t *start;
    double *latencies_ms;
    long sent;
    long errors;
} producer_t;



typedef struct{
    const bench_config_t *config;
    pthread_barrier_t *start;
    long consumed;
    double last_message_ms;
    int failed;
} consumer_t;



static void usage(const char *program){
    fprintf(stderr,
            "usage: %s <host> <port> <topic> <producers> <messages> <bytes> <0|1|ALL> <consumers> <group> [hot_percent]\n"
            "  producers    threads that produce, each with its own connection\n"
            "  messages     messages per producer\n"
            "  bytes        size of each message\n"
            "  consumers    members of <group> that consume from the beginning\n"
            "  hot_percent  percentage of messages sent with the same key (default 0)\n"
            "output (CSV): producers,consumers,messages,bytes,acks,produced,seconds,msg_s,mb_s,"
            "latency_median_ms,latency_p99_ms,consumed,consume_msg_s\n", program);
}




static int parse_number(const char *text, long min, long max, long *out){
    char *end = NULL;
    errno = 0;
    long value = strtol(text, &end, 10);

    if(errno != 0 || end == text || *end != '\0' || value < min || value > max)
        return -1;

    *out = value;
    return 0;
}



static double now_ms(void){
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (double)now.tv_sec * 1000.0 + (double)now.tv_nsec / 1000000.0;
}




static int parse_config(int argc, char **argv, bench_config_t *config){
    if(argc != 10 && argc != 11)
        return -1;

    long port;
    long producers;
    long messages;
    long bytes;
    long consumers;
    long hot = 0;

    if(parse_number(argv[2], 1, 65535, &port) < 0
       || parse_number(argv[4], 0, 1024, &producers) < 0
       || parse_number(argv[5], 1, 100000000, &messages) < 0
       || parse_number(argv[6], 1, MAX_VALUE_BYTES, &bytes) < 0
       || parse_number(argv[8], 0, 1024, &consumers) < 0
       || (argc == 11 && parse_number(argv[10], 0, 100, &hot) < 0))


        return -1;



    if(strcmp(argv[7], "0") == 0)
        config->acks = ACKS_NONE;

    else if(strcmp(argv[7], "1") == 0)
        config->acks = ACKS_LEADER;

    else if(strcmp(argv[7], "ALL") == 0)
        config->acks = ACKS_ALL;

    else
        return -1;

    if(producers == 0 && consumers == 0)
        return -1;   

    config->host = argv[1];
    config->port = (unsigned short)port;
    config->topic = argv[3];
    config->producers = (int)producers;
    config->messages = messages;
    config->message_bytes = (size_t)bytes;
    config->consumers = (int)consumers;
    config->group = argv[9];
    config->hot_percent = (int)hot;


    return 0;
}




static void *run_producer(void *arg){

    producer_t *producer = arg;
    const bench_config_t *config = producer->config;

    mb_client_t *client = mbc_connect(config->host, config->port);
    char *value = malloc(config->message_bytes);


    if(value != NULL)
        memset(value, 'x', config->message_bytes);


    pthread_barrier_wait(producer->start);

    if(client == NULL || value == NULL){
        producer->errors = config->messages;
        free(value);
        mbc_close(client);
        return NULL;
    }

    for(long i = 0; i < config->messages; i++){
        const char *key = NULL;
        size_t key_len = 0;

        //hot_percent of every 100 messages go with the same key, so they land in one partition
        if(i % 100 < config->hot_percent){
            key = HOT_KEY;
            key_len = strlen(HOT_KEY);
        }


        int partition;
        long offset;
        double before = now_ms();
        int status = mbc_produce(client, config->topic, key, key_len, value, config->message_bytes, config->acks, &partition, &offset);
        double after = now_ms();


        if(status != MB_OK){
            producer->errors++;

            if(status == -1)
                break;

            continue;
        }


        producer->latencies_ms[producer->sent] = after - before;
        producer->sent++;
    }

    free(value);
    mbc_close(client);
    return NULL;
}





static void *run_consumer(void *arg){

    consumer_t *consumer = arg;
    const bench_config_t *config = consumer->config;

    mb_client_t *client = mbc_connect(config->host, config->port);

    pthread_barrier_wait(consumer->start);

    if(client == NULL){
        consumer->failed = 1;
        return NULL;
    }

    int status = mbc_join(client, config->topic, config->group, 1);
    double last = now_ms();


    while(status == MB_OK && now_ms() - last < CONSUMER_IDLE_MS){
        message_t *msgs;
        size_t n;
        int partition;

        status = mbc_poll(client, POLL_MAX_BYTES, POLL_WAIT_MS, &msgs, &n, &partition);


        if(status == MB_OK){
            long next = 0;

            if(n > 0){
                next = (long)msgs[n - 1].offset + 1;
                consumer->consumed += (long)n;
                last = now_ms();
                consumer->last_message_ms = last;
            }

            free(msgs);

            if(n > 0)
                status = mbc_commit(client, partition, next);
        }


        if(status == MB_ERR_REBALANCE)
            status = mbc_join(client, config->topic, config->group, 1);
    }

    if(status != MB_OK)
        consumer->failed = 1;

    mbc_close(client);
    return NULL;
}



static int compare_doubles(const void *a, const void *b){
    double x = *(const double *)a;
    double y = *(const double *)b;

    if(x < y)
        return -1;

    if(x > y)
        return 1;

    return 0;
}



static double percentile(const double *sorted, long n, double fraction){
    if(n == 0)
        return 0.0;

    return sorted[(long)(fraction * (double)(n - 1))];
}



static const char *acks_text(acks_t acks){
    if(acks == ACKS_NONE)
        return "0";

    if(acks == ACKS_LEADER)
        return "1";

    return "ALL";
}





int main(int argc, char **argv){
    bench_config_t config;

    if(parse_config(argc, argv, &config) < 0){
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    producer_t *producers = calloc((size_t)config.producers + 1, sizeof *producers);
    consumer_t *consumers = calloc((size_t)config.consumers + 1, sizeof *consumers);
    pthread_t *threads = calloc((size_t)(config.producers + config.consumers), sizeof *threads);
    double *latencies = malloc(((size_t)config.producers * (size_t)config.messages + 1) * sizeof *latencies);


    if(producers == NULL || consumers == NULL || threads == NULL || latencies == NULL){
        fprintf(stderr, "out of memory\n");
        return EXIT_FAILURE;
    }


    //Every thread and main wait here
    pthread_barrier_t start;
    pthread_barrier_init(&start, NULL, (unsigned)(config.producers + config.consumers + 1));



    for(int i = 0; i < config.producers; i++){

        producers[i].config = &config;
        producers[i].start = &start;
        producers[i].latencies_ms = latencies + (size_t)i * (size_t)config.messages;


        if(pthread_create(&threads[i], NULL, run_producer, &producers[i]) != 0){
            fprintf(stderr, "pthread_create failed\n");
            exit(EXIT_FAILURE);
        }
    }


    for(int i = 0; i < config.consumers; i++){

        consumers[i].config = &config;
        consumers[i].start = &start;

        if(pthread_create(&threads[config.producers + i], NULL, run_consumer, &consumers[i]) != 0){
            fprintf(stderr, "pthread_create failed\n");
            exit(EXIT_FAILURE);
        }
    }


    pthread_barrier_wait(&start);
    double start_ms = now_ms();

    for(int i = 0; i < config.producers; i++)
        pthread_join(threads[i], NULL);


    double produce_end_ms = now_ms();

    for(int i = 0; i < config.consumers; i++)
        pthread_join(threads[config.producers + i], NULL);



    pthread_barrier_destroy(&start);


    //PRODUCERS
    long produced = 0;
    long errors = 0;


    for(int i = 0; i < config.producers; i++){
        for(long j = 0; j < producers[i].sent; j++)
            latencies[produced++] = producers[i].latencies_ms[j];

        errors += producers[i].errors;
    }


    qsort(latencies, (size_t)produced, sizeof *latencies, compare_doubles);


    double seconds = (produce_end_ms - start_ms) / 1000.0;
    double msg_s = 0.0;
    double mb_s = 0.0;

    if(produced > 0 && seconds > 0.0){
        msg_s = (double)produced / seconds;
        mb_s = (double)produced * (double)config.message_bytes / (1024.0 * 1024.0) / seconds;
    }




    //CONSUMERS
    long consumed = 0;
    double last_ms = start_ms;


    for(int i = 0; i < config.consumers; i++){
        consumed += consumers[i].consumed;

        if(consumers[i].last_message_ms > last_ms)
            last_ms = consumers[i].last_message_ms;

        if(consumers[i].failed)
            errors++;
    }

    double consume_msg_s = 0.0;

    if(consumed > 0 && last_ms > start_ms)
        consume_msg_s = (double)consumed / ((last_ms - start_ms) / 1000.0);


    fprintf(stderr, "produced %ld messages in %.3f s (%.1f msg/s, %.3f MB/s), latency median %.3f ms, p99 %.3f ms\n" "consumed %ld messages (%.1f msg/s), %ld errors\n",
            produced, seconds, msg_s, mb_s, percentile(latencies, produced, 0.5), percentile(latencies, produced, 0.99), consumed, consume_msg_s, errors);

    printf("%d,%d,%ld,%zu,%s,%ld,%.3f,%.1f,%.3f,%.3f,%.3f,%ld,%.1f\n",
           config.producers, config.consumers, config.messages, config.message_bytes, acks_text(config.acks),
           produced, seconds, msg_s, mb_s, percentile(latencies, produced, 0.5), percentile(latencies, produced, 0.99), consumed, consume_msg_s);



    free(latencies);
    free(threads);
    free(consumers);
    free(producers);


    if(errors > 0)
        return EXIT_FAILURE;



    return EXIT_SUCCESS;
}