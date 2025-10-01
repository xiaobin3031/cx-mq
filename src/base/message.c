#include "message.h"
#include <stdlib.h>

static int closed = 0;

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

int enqueue_message(MessageQueue* queue, Message* msg) {
    if (queue->size >= queue->capacity) {
        size_t new_capacity = (queue->capacity == 0) ? 10 : queue->capacity * 2;
        Message** new_msgs = (Message**)realloc(queue->msgs, new_capacity * sizeof(Message*));
        if (!new_msgs) return -1; // realloc failed

        queue->msgs = new_msgs;
        queue->capacity = new_capacity;
    }

    queue->msgs[queue->size++] = msg;
    return 0;
}

Message* dequeue_message(MessageQueue* queue) {
    if (!queue || queue->size == 0 || queue->pos >= queue->size) return NULL;

    Message* msg = queue->msgs[queue->pos];
    queue->msgs[queue->pos] = NULL;
    queue->pos++;

    return msg;
}

Message* get_message_by_id(MessageQueue* queue, uint64_t id) {
    if (!queue) return NULL;

    for (size_t i = 0; i < queue->size; i++) {
        if (queue->msgs[i]->id == id) {
            return queue->msgs[i];
        }
    }
    return NULL;
}


void close_message() {
    if(closed) return;

    closed = 1;
}