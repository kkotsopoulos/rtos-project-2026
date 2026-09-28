#ifndef CONSUMER_H
#define CONSUMER_H

// dequeues JSON from buffer, parses with cJSON, increments counters
// exits when cb_dequeue() returns CB_ERR_SHUT
void *consumer_thread_fn(void *arg);

#endif
