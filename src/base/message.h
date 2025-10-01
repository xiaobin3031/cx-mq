#pragma once

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#define CLOSED -1001
#define ARG_EMPTY -1002
#define QUEUE_EMPTY -1003

typedef struct {
    uint64_t id;

    size_t len;
    char* data;

    char* topic;
    char* group;
    long timestamp;

    // 已经保存到文件中
    uint8_t logged;
} Message;

typedef struct {
    uint64_t id;

    Message** msgs;
    size_t size;
    size_t capacity;
    size_t pos;

    pthread_mutex_t mutex;
} MessageQueue;

typedef struct {
    int fd;

    char* topic;
    char* group;
} Client;

typedef struct {
    Client** clients;

    size_t size;
    size_t capacity;

    pthread_t tid;
} SocketQueue;

MessageQueue* create_message_queue();
void destroy_message_queue(MessageQueue* queue);
int enqueue_message(MessageQueue* queue, Message* msg);
Message* dequeue_message(MessageQueue* queue);
Message* get_message_by_id(MessageQueue* queue, uint64_t id);

SocketQueue* start_message_service();
void destroy_socket_queue(SocketQueue* socket_queue);

// 产生id
uint64_t generate_id();


// 产生消息
uint64_t produce(MessageQueue *queue, const char* topic, const char* group, const char* data, size_t len);
// 消费消息
int consume(MessageQueue* queue, SocketQueue* socket_queue);

void close_producer();
void close_consumer();
void close_message();