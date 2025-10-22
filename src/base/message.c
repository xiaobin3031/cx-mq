#include "message.h"
#include <stdlib.h>
#include <stdio.h>

static int closed = 0;

/**
 * @brief Create a message queue object
 * 
 * @return MessageQueue* 
 */
MessageQueue* create_message_queue() {
    MessageQueue* queue = (MessageQueue*)malloc(sizeof(MessageQueue));
    if (!queue) return NULL;

    queue->id = generate_id();
    queue->msgs = NULL;
    queue->size = 0;
    queue->capacity = 0;
    queue->pos = 0;

    return queue;
}

/**
 * @brief Destroy a message queue object
 * 
 * @param queue 
 */
void destroy_message_queue(MessageQueue* queue) {
    if (!queue) return;

    for (size_t i = 0; i < queue->size; i++) {
        free(queue->msgs[i]->data);
        free(queue->msgs[i]->topic);
        free(queue->msgs[i]->group);
        free(queue->msgs[i]);
    }
    free(queue->msgs);
    free(queue);
}

/**
 * @brief Enqueue a message to the queue
 * 
 * @param queue 
 * @param msg 
 * @return int 
 */
int enqueue_message(MessageQueue* queue, Message* msg) {
    if (queue->size >= queue->capacity) {
        size_t new_capacity = (queue->capacity == 0) ? 10 : queue->capacity * 2;
        Message** new_msgs = (Message**)realloc(queue->msgs, new_capacity * sizeof(Message*));
        if (!new_msgs) return -1; // realloc failed

        queue->msgs = new_msgs;
        queue->capacity = new_capacity;
    }

    queue->msgs[queue->size++] = msg;
    printf("Message enqueued: ID=%lu, Topic=%s, Group=%s, total messages: %zu\n", msg->id, msg->topic, msg->group, queue->size);
    return 0;
}

/**
 * @brief Dequeue a message from the queue
 * 
 * @param queue 
 * @return Message* 
 */
Message* dequeue_message(MessageQueue* queue) {
    if (!queue || queue->size == 0 || queue->pos >= queue->size) return NULL;

    Message* msg = queue->msgs[queue->pos];
    queue->msgs[queue->pos] = NULL;
    queue->pos++;

    return msg;
}

/**
 * @brief Get the message by id object
 * 
 * @param queue 
 * @param id 
 * @return Message* 
 */
Message* get_message_by_id(MessageQueue* queue, uint64_t id) {
    if (!queue) return NULL;

    for (size_t i = 0; i < queue->size; i++) {
        if (queue->msgs[i]->id == id) {
            return queue->msgs[i];
        }
    }
    return NULL;
}

void print_message(Message* msg) {
    if (!msg) {
        printf("Message is NULL\n");
        return;
    }
    printf("Message ID: %lu, Topic: %s, Group: %s, Timestamp: %ld, Data: %.*s\n",
           msg->id, msg->topic, msg->group, msg->timestamp, (int)msg->len, msg->data);
}

/**
 * @brief 
 * 
 */
void close_message() {
    if(closed) return;

    closed = 1;
}