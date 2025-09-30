#include "message.h"
#include <stdint.h>
#include <pthread.h>

static uint64_t counter = 0;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

uint64_t generate_id() {

    pthread_mutex_lock(&lock);
    counter++;
    pthread_mutex_unlock(&lock);

    return counter;
    // Use the counter as needed, e.g., assign it to a message ID
}