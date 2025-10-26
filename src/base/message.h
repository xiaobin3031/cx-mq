#pragma once

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#define CLOSED -1001
#define ARG_EMPTY -1002
#define QUEUE_EMPTY -1003

typedef enum {
    PRODUCE     = 0x0001,       // Client -> Server
    CONSUME     = 0x0002,       // Client -> Server
    ACK         = 0x0003,       // ACK 消费成功  Client -> Server
    NACK        = 0x0004,       // NACK消费失败  Client -> Server
    MESSAGE     = 0x0005,       // 消息     Server -> Client
    HEARTBEAT   = 0x00FF,     // 心跳
} MessageType;

typedef enum {
    GROUP           = 0x01,
    TOPIC           = 0x02,
    MESSAGE_ID      = 0x03,
    DELAY_LEVEL     = 0x04,


    PAYLOAD_END     = 0xFF,
} PayloadType;

typedef struct {
    uint16_t version;
    uint16_t type;          // 消息类型
    uint32_t seq_id;
    uint64_t msg_len;
    uint32_t attr_len;
    uint32_t reserved;
} MsgHeader;

typedef struct {
    MsgHeader *header;
    char* message;
    Attrs *attrs;
} MsgBody;

typedef struct {
    char* group;
    char* topic;

    char* message_id;

    uint8_t delay_level;

    uint64_t len;
    char* message;
} Attrs;

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
    pthread_t tid;
} MessageQueue;

typedef struct {
    int fd;
    pthread_t tid;

    char* topic;
    char* group;
} Client;

typedef struct {
    Client** clients;

    size_t size;
    size_t capacity;

    pthread_t tid;
} SocketQueue;

typedef struct {
    MessageQueue *queue;
    SocketQueue* socket_queue;
} ConsumeArgs;

MessageQueue* create_message_queue();
void destroy_message_queue(MessageQueue* queue);
int enqueue_message(MessageQueue* queue, Message* msg);
Message* dequeue_message(MessageQueue* queue);
Message* get_message_by_id(MessageQueue* queue, uint64_t id);
void print_message(Message* msg);

int start_message_service();
void destroy_socket_queue(SocketQueue* socket_queue);

// 产生id
uint64_t generate_id();


// 产生消息
uint64_t produce(MessageQueue *queue, const char* topic, const char* group, const char* data, size_t len);
int start_consumer(MessageQueue* queue, SocketQueue* socket_queue);

void close_producer();
void close_consumer();
void close_message();

char* get_result_info(int result);