#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>

#ifdef __APPLE__
#include <arpa/inet.h>
#define be64toh ntohll
#define htobe64 htonll
#endif

static int port = 12345;
/**
 * @brief 启动命令行客户端，用于测试消费者功能
 * 
 * @return int 
 */
int main() {

    // 连接到服务器，把自己注册为消费者
    // 读取消息并打印
    int sockfd;
    struct sockaddr_in server_addr;
    char *message = "C:test_topic:test_group";  // 消费者标识，格式为C:topic:group
    char buffer[1024];
    // 创建socket
    printf("Starting consumer client...\n");
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed");
        return -1;
    }
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    server_addr.sin_port = htons(port);
    // 连接服务器
    printf("Connected to server on port %d\n", port);
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection to server failed");
        close(sockfd);
        return -1;
    }
    // 发送消费者标识
    printf("Subscribed to topic 'test_topic' and group 'test_group'\n");
    send(sockfd, message, strlen(message), 0);

    // 读取返回值
    ssize_t bytes_received = recv(sockfd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received <= 0) {
        printf("Connection closed by server or error occurred\n");
        close(sockfd);
        return -1;
    }
    buffer[bytes_received] = '\0';
    printf("Server response: %s\n", buffer);
    if(strcmp(buffer, "OK.") != 0) {
        // 服务端没有正确回应
        printf("Connection closed by server or error occurred\n");
        close(sockfd);
        return -1;
    }

    // 接收消息
    while (1) {
        uint64_t data_len;
        printf("Waiting for messages...\n");
        ssize_t len_read = read(sockfd, &data_len, sizeof(data_len));
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
        ssize_t data_read = read(sockfd, data_buffer, data_len);
        if (data_read != data_len) {
            free(data_buffer);
            break;
        }
        printf("Received message: %s\n", data_buffer);
    }
    close(sockfd);

    return 0;
}