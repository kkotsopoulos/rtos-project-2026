#include "globals.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

app_state_t g_app;

// volatile sig_atomic_t: safe to write from signal handler
volatile sig_atomic_t g_shutdown = 0;

// initialize shared counters and lock
int counters_init(msg_counters_t *c) {
  if (c == NULL) {
    return -1;
  }

  memset(c, 0, sizeof(msg_counters_t));

  // mutex for catching bugs
  pthread_mutexattr_t attr;
  int ret = pthread_mutexattr_init(&attr);
  if (ret != 0) {
    fprintf(stderr, "[counters_init] pthread_mutexattr_init: %s\n",
            strerror(ret));
    return -1;
  }

  ret = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
  if (ret != 0) {
    fprintf(stderr, "[counters_init] pthread_mutexattr_settype: %s\n",
            strerror(ret));
    pthread_mutexattr_destroy(&attr);
    return -1;
  }

  ret = pthread_mutex_init(&c->counters_mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  // check if mutex initialization was successful
  if (ret != 0) {
    fprintf(stderr, "[counters_init] pthread_mutex_init: %s\n", strerror(ret));
    return -1;
  }

  return 0;
}

void counters_destroy(msg_counters_t *c) {
  if (c == NULL) {
    return;
  }

  fprintf(stderr,
          "[counters] Final totals: commit=%llu identity=%llu "
          "account=%llu info=%llu unknown=%llu\n",
          (unsigned long long)c->total_count[MSG_KIND_COMMIT],
          (unsigned long long)c->total_count[MSG_KIND_IDENTITY],
          (unsigned long long)c->total_count[MSG_KIND_ACCOUNT],
          (unsigned long long)c->total_count[MSG_KIND_INFO],
          (unsigned long long)c->unknown_count);

  pthread_mutex_destroy(&c->counters_mutex);
}

// hot path — lock is held for ~3 instructions (read, add, write)
void counters_increment(msg_counters_t *c, msg_kind_t kind) {
  if (c == NULL || kind >= MSG_KIND_COUNT) {
    return;
  }

  // separate mutex from buffer.mutex — consumer can increment while producer
  // enqueues
  pthread_mutex_lock(&c->counters_mutex);
  c->window_count[kind]++;
  c->total_count[kind]++;
  pthread_mutex_unlock(&c->counters_mutex);
}

// called by monitor every 1s — reads and zeros window_count[] atomically
void counters_snapshot_and_reset(msg_counters_t *c,
                                 uint64_t out[MSG_KIND_COUNT]) {
  if (c == NULL || out == NULL) {
    return;
  }

  pthread_mutex_lock(&c->counters_mutex);

  for (int i = 0; i < MSG_KIND_COUNT; i++) {
    out[i] = c->window_count[i];
    c->window_count[i] = 0;
  }

  pthread_mutex_unlock(&c->counters_mutex);
}
