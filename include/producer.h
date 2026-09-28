#ifndef PRODUCER_H
#define PRODUCER_H

#include <stdbool.h>
#include <stddef.h>

#define LWS_HOST "jetstream1.us-east.bsky.network"
#define LWS_PORT 443
#define LWS_PATH "/subscribe?wantedCollections=app.bsky.feed.post"
#define LWS_PROTO_NAME "rtos-firehose-proto"
#define LWS_SERVICE_TIMEOUT_MS 50
#define LWS_MAX_FRAME_SIZE (64 * 1024) // 64 KB max

#define RECONNECT_DELAY_MS_INIT 1000
#define RECONNECT_DELAY_MS_MAX 30000 //  backoff cap

typedef struct {
  int _reserved;
} producer_args_t;

// websocket producer thread — connects to Jetstream, enqueues JSON into buffer
// auto-reconnects with exponential backoff on disconnect
// exits on g_shutdown == 1
void *producer_thread_fn(void *arg);

#endif
