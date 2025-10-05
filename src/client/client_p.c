#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>

static int port = 12345;

/**
 * @brief 启动命令行客户端，用于测试生产者功能
 * 
 * @return int 
 */
int main() {

    // 连接到服务器，把自己注册为生产者
    // 发送消息后断开连接
    int sockfd;
    struct sockaddr_in server_addr;
    char *hello_message = "P:test_topic:test_group";  // 生产者标识，格式为P:topic:group:data

    // 创建socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Socket creation failed");
        return -1;
    }
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    server_addr.sin_port = htons(port);
    // 连接服务器
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection to server failed");
        close(sockfd);
        return -1;
    }
    printf("Connected to server on port %d\n", port);
    // 发送生产者标识
    send(sockfd, hello_message, strlen(hello_message), 0);

    printf("Registered as producer for topic 'test_topic' and group 'test_group'\n");

    // 读取返回值
    char buffer[1024];
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

    // 循环读取用户输入并发送消息
    while (1) {
        printf("Enter message to send (or 'exit' to quit): ");
        if (fgets(buffer, sizeof(buffer), stdin) == NULL) {
            break; // EOF or error
        }
        // 去掉换行符
        size_t len = strlen(buffer);
        if (len > 0 && buffer[len - 1] == '\n') {
            buffer[len - 1] = '\0';
            len--;
        }
        if (strcmp(buffer, "exit") == 0) {
            break; // Exit command
        }
        // 在buffer前加上长度信息，用uint64_t表示
        uint64_t data_len = len;
        // 用网络字节序发送长度
        data_len = htobe64(data_len);
        // 发送长度
        if (send(sockfd, &data_len, sizeof(data_len), 0) < 0) {
            perror("Send data length failed");
            break;
        }
        // 发送消息内容
        if (send(sockfd, buffer, len, 0) < 0) {
            perror("Send failed");
            break;
        }
        printf("Message sent: %s\n", buffer);
    }
    close(sockfd);

    return 0;
}