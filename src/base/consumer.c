#include "message.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

static uint8_t closed = 0;
static uint16_t head_count = 4; // 头部字段数量

static int client_match(Client* client, Message* msg) {
    return strcmp(client->topic, msg->topic) == 0 && strcmp(client->group, msg->group) == 0;
}

static int send(int fd, Message* msg) {
    size_t head_offset = 0, body_offset = 0;
    size_t head_len = sizeof(uint16_t) + head_count * sizeof(uint32_t);



    // 这里简单示例，实际应用中应处理发送失败等情况
    size_t total_len = sizeof(msg->id) + sizeof(msg->len) + msg->len;
    char* buffer = (char*)malloc(total_len);
    if (!buffer) return -1; // Memory allocation failed

    memcpy(buffer, &msg->id, sizeof(msg->id));
    memcpy(buffer + sizeof(msg->id), &msg->len, sizeof(msg->len));
    memcpy(buffer + sizeof(msg->id) + sizeof(msg->len), msg->data, msg->len);

    ssize_t sent = write(fd, buffer, total_len);
    free(buffer);
    return (sent == total_len) ? 0 : -1; // Return 0 on success
}

uint64_t consume(MessageQueue* queue, SocketQueue* socket_queue) {
    if(closed) return CLOSED;
    if(!queue) return QUEUE_EMPTY;

    // 根据queue->id，加一个消费锁
    pthread_mutex_lock(&queue->mutex);

    Message* msg;
    while ((msg = dequeue_message(queue)) != NULL) {
        // 处理消息
        // 这里简单打印消息内容，实际应用中应有更复杂的处理逻辑
        printf("Consumed Message ID: %lu, Topic: %s, Group: %s, Data: %.*s\n",
               msg->id, msg->topic, msg->group, (int)msg->len, msg->data);

        // 发送消息给所有连接的客户端
        for (size_t i = 0; i < socket_queue->size; i++) {
            Client* client = socket_queue->clients[i];
            if (client_match(client, msg)) {
                send(client->fd, msg);
            }
        }

        // 释放消息内存
        free(msg->data);
        free(msg->topic);
        free(msg->group);
        free(msg);
    }

    pthread_mutex_unlock(&queue->mutex);
}