#include "message.h"
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>

static FILE* log_file = NULL;
static uint8_t closed = 0;

// 保存message到文件
static void log_message_to_file(Message* msg) {
    if(msg->logged) return; // 已经保存过了

    // 这里简单示例，实际应用中应处理文件打开失败等情况
    if(!log_file) {
        log_file = fopen("messages.log", "a+");
        if(!log_file) return; // 文件打开失败
    }
    if (log_file) {
        fprintf(log_file, "ID: %lu, Topic: %s, Group: %s, Timestamp: %ld, Data: %.*s\n",
                msg->id, msg->topic, msg->group, msg->timestamp, (int)msg->len, msg->data);
        msg->logged = 1; // 标记为已保存
    }
}

uint64_t produce(MessageQueue *queue, const char* topic, const char* group, const char* data, size_t len) {
    if(closed) return CLOSED;
    if(!queue) return QUEUE_EMPTY;
    if (!topic || !group || !data || len == 0) return ARG_EMPTY;

    Message* msg = (Message*)malloc(sizeof(Message));
    if (!msg) return 0; // Memory allocation failed

    msg->id = generate_id();
    msg->len = len;
    msg->data = (char*)malloc(len);
    if (!msg->data) {
        free(msg);
        return 0; // Memory allocation failed
    }
    memcpy(msg->data, data, len);

    msg->topic = strdup(topic);
    msg->group = strdup(group);
    msg->timestamp = time(NULL);
    msg->logged = 0;

    log_message_to_file(msg);
    enqueue_message(queue, msg);

    return msg->id;
}

void close_producer() {
    if(closed) return;
    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }
    closed = 1;
}