#include "message.h"
#include <stdlib.h>
#include <pthread.h>

static uint8_t closed = 0;

static SocketQueue* create_socket_queue() {
    SocketQueue* socket_queue = (SocketQueue*)malloc(sizeof(SocketQueue));
    if (!socket_queue) return NULL;

    socket_queue->clients = NULL;
    socket_queue->size = 0;
    socket_queue->capacity = 0;

    return socket_queue;
}

void destroy_socket_queue(SocketQueue* socket_queue) {
    if (!socket_queue) return;

    for (size_t i = 0; i < socket_queue->size; i++) {
        free(socket_queue->clients[i]->topic);
        free(socket_queue->clients[i]->group);
        free(socket_queue->clients[i]);
    }
    free(socket_queue->clients);
    free(socket_queue);
}

static int enqueue_socket_client(SocketQueue* socket_queue, Client* client) {
    if (socket_queue->size >= socket_queue->capacity) {
        size_t new_capacity = (socket_queue->capacity == 0) ? 10 : socket_queue->capacity * 2;
        Client** new_clients = (Client**)realloc(socket_queue->clients, new_capacity * sizeof(Client*));
        if (!new_clients) return -1; // realloc failed

        socket_queue->clients = new_clients;
        socket_queue->capacity = new_capacity;
    }

    socket_queue->clients[socket_queue->size++] = client;
    return 0;
}

static void* client_accept_thread(void* arg) {
    SocketQueue* queue = (SocketQueue*)arg;
    while (1) {
        Client* client = accept_client_connection();
        if (!client) continue;
        enqueue_socket_client(queue, client);
    }
    return NULL;
}

SocketQueue* start_message_service() {
    // 启动消息服务的逻辑
    // 例如，初始化网络监听，处理客户端连接等
    SocketQueue* socket_queue = create_socket_queue();

    pthread_t tid;
    pthread_create(&tid, NULL, client_accept_thread, socket_queue);
    socket_queue->tid = tid;

    return socket_queue;
}