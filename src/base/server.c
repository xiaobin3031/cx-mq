#include "message.h"
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>

#ifdef __APPLE__
#include <arpa/inet.h>
#define be64toh ntohll
#else
#include <endian.h>
#endif

static uint8_t closed = 0;
static SocketQueue* socketQueue = NULL;
static MessageQueue* messageQueue = NULL;

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
    printf("Client enqueued: FD=%d, Topic=%s, Group=%s, client total: %zu\n", client->fd, client->topic, client->group, socket_queue->size);
    return 0;
}

static void destroy_client(Client* client) {
    if (!client) return;

    if(client->topic) free(client->topic);
    if(client->group) free(client->group);
    if(client->fd > 0) close(client->fd);
    free(client);
}

static void* client_produce_thread(void* arg) {
    Client* client = (Client*)arg;
    // 处理生产者连接的线程
    while(1) {
        uint64_t data_len;
        printf("Waiting for message from producer FD %d...\n", client->fd);
        ssize_t len_read = read(client->fd, &data_len, sizeof(data_len));
        // 读取的长度是网络字节序，转换成真正的长度
        data_len = be64toh(data_len);
        printf("Received message length: %lu <-> %lu\n", data_len, len_read);
        if (len_read != sizeof(data_len) || data_len == 0 || data_len > 1024*1024) { // 限制最大1MB
            break;
        }
        char* data_buffer = (char*)malloc(data_len);
        if (!data_buffer) {
            break;
        }
        ssize_t data_read = read(client->fd, data_buffer, data_len);
        if (data_read != data_len) {
            free(data_buffer);
            break;
        }
        printf("Received message from producer FD %d: Topic=%s, Group=%s, Data: %.*s\n", client->fd, client->topic, client->group, (int)data_len, data_buffer);
        uint64_t mq_id = produce(messageQueue, client->topic, client->group, data_buffer, data_len);
        // 把mq_id返回给生产者
        char id_buffer[32];
        int id_len = snprintf(id_buffer, sizeof(id_buffer), "ID:%lu", mq_id);
        send(client->fd, id_buffer, id_len, 0);
        printf("Message produced with ID %lu\n", mq_id);
        free(data_buffer);
        int result = start_consumer(messageQueue, socketQueue); // 启动消费者线程
        printf("start consumer result: %d(%s)\n", result, get_result_info(result));
    }
    // 生产者连接后直接关闭
    printf("Producer connected and disconnected: FD=%d\n", client->fd);
    destroy_client(client);
    return NULL;
}

static void* client_accept_thread(void* arg) {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);

    // 创建socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed");
        return NULL;
    }
    // 绑定地址和端口
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(12345); // 监听12345端口
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_fd);
        return NULL;
    }
    // 监听连接
    if (listen(server_fd, 5) < 0) {
        perror("Listen failed");
        close(server_fd);
        return NULL;
    }

    printf("Message service started, listening on port 12345\n");
    while(1) {
        // 等待客户端连接
        if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len)) < 0) {
            perror("Accept failed");
            continue;
        }
        printf("New client connected: FD=%d\n", client_fd);
        Client* client = (Client*)malloc(sizeof(Client));
        if (!client) {
            close(client_fd);
            continue;
        }
        client->fd = client_fd;

        // 读取输入流，获取topic和group，第一位标记是produer还是consumer
        // 这里简单示例，实际应用中应处理读取失败等情况
        char buffer[256];
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            destroy_client(client);
            continue;
        }
        buffer[bytes_read] = '\0';
        // 格式为 格式为C/P:topic:group
        printf("received from client FD %d: %s\n", client_fd, buffer);
        char type = buffer[0];
        if (type != 'C' && type != 'P') {
            destroy_client(client);
            continue;
        }
        // topic和group用:分割
        char* sep1 = strchr(buffer + 1, ':');
        if (!sep1) {
            destroy_client(client);
            continue;
        }
        *sep1 = '\0';
        char* sep2 = strchr(sep1 + 1, ':');
        if (!sep2) {
            destroy_client(client);
            continue;
        }
        *sep2 = '\0';
        client->topic = strdup(buffer + 1);
        client->group = strdup(sep1 + 1);
        if (!client->topic || !client->group) {
            destroy_client(client);
            continue;
        }
        send(client_fd, "OK.", 3, 0); // 简单回复
        if(type == 'C') {
            // consumer
            printf("Registering new consumer: FD=%d, Topic=%s, Group=%s\n", client->fd, client->topic, client->group);
            enqueue_socket_client(socketQueue, client);
            printf("Consumer registered: FD=%d, Topic=%s, Group=%s, total consumers: %zu\n", client->fd, client->topic, client->group, socketQueue->size);
            start_consumer(messageQueue, socketQueue); // 启动消费者线程
        } else {
            // producer
            // 重新读取长度, 不从buffer中读取，防止长度不够
            pthread_t tid;
            if (pthread_create(&tid, NULL, client_produce_thread, client) != 0) {
                destroy_client(client);
                continue;
            }
            client->tid = tid;
        }
    }
}

int start_message_service() {
    if (closed) return CLOSED;

    socketQueue = create_socket_queue();
    messageQueue = create_message_queue();
    if (!socketQueue || !messageQueue) {
        free(socketQueue);
        free(messageQueue);
        return QUEUE_EMPTY;
    }
    // 启动消息服务的逻辑
    // 例如，初始化网络监听，处理客户端连接等

    // 启动一个线程专门处理客户端连接
    /*if (pthread_create(&socketQueue->tid, NULL, client_accept_thread, NULL) != 0) {
        destroy_socket_queue(socketQueue);
        destroy_message_queue(messageQueue);
        return -1; // Thread creation failed
    }*/
   client_accept_thread(NULL);

    return 0;
}