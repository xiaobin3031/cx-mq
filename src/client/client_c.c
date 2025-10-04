#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>

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
    // 接收消息
    while (1) {
        printf("Waiting for messages...\n");
        ssize_t bytes_received = recv(sockfd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            printf("Connection closed by server or error occurred\n");
            break;
        }
        buffer[bytes_received] = '\0';
        printf("Received message: %s\n", buffer);
    }
    close(sockfd);

    return 0;
}