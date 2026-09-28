#ifndef GLOBALS_H
#define GLOBALS_H

#include "circular_buffer.h"
#include <pthread.h>
#include <signal.h>
#include <stdint.h>

// Jetstream message types — used as array index in counters
typedef enum {
  MSG_KIND_COMMIT = 0,
  MSG_KIND_IDENTITY = 1,
  MSG_KIND_ACCOUNT = 2,
  MSG_KIND_INFO = 3,
  MSG_KIND_UNKNOWN = 4, // not tracked separately
  MSG_KIND_COUNT = 4,   // number of tracked kinds
} msg_kind_t;

#define KIND_STR_COMMIT "commit"
#define KIND_STR_IDENTITY "identity"
#define KIND_STR_ACCOUNT "account"
#define KIND_STR_INFO "info"

typedef struct {
  uint64_t window_count[MSG_KIND_COUNT]; // last 1s, protected by counters_mutex
  uint64_t total_count[MSG_KIND_COUNT];  // cumulative
  uint64_t unknown_count;
  pthread_mutex_t
      counters_mutex; // separate from buffer mutex to cut contention
} msg_counters_t;

typedef struct {
  circular_buffer_t buffer; // producer -> consumer
  msg_counters_t counters;  // consumer -> monitor
} app_state_t;

extern app_state_t g_app;
extern volatile sig_atomic_t g_shutdown; // set to 1 by signal handler

int counters_init(msg_counters_t *c);
void counters_destroy(msg_counters_t *c);

// called from consumer hot path, locks counters_mutex internally
void counters_increment(msg_counters_t *c, msg_kind_t kind);

// atomically reads and zeros window_count[]
// called by monitor every 1s
void counters_snapshot_and_reset(msg_counters_t *c,
                                 uint64_t out[MSG_KIND_COUNT]);

#endif /* GLOBALS_H */
