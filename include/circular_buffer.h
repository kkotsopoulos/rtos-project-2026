#ifndef CIRCULAR_BUFFER_H
#define CIRCULAR_BUFFER_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// power of 2
#define BUFFER_CAPACITY 256u
#define BUFFER_MASK (BUFFER_CAPACITY - 1u)

_Static_assert((BUFFER_CAPACITY & BUFFER_MASK) == 0u,
               "BUFFER_CAPACITY must be a power of 2");

typedef enum {
  CB_OK = 0,
  CB_ERR_FULL = -1,  // buffer full, message dropped
  CB_ERR_NULL = -2,  // NULL pointer arg
  CB_ERR_INVAL = -3, // not initialized or internal error
  CB_ERR_SHUT = -4,  // shutdown mode
} cb_status_t;

// stats snapshot for monitor/logger
typedef struct {
  unsigned int count;
  unsigned int capacity;
  uint64_t total_enqueued;
  uint64_t total_dropped;
  float occupancy_pct;
} cb_stats_t;

typedef struct {
  char *slots[BUFFER_CAPACITY];

  unsigned int head; // next dequeue position (consumer)
  unsigned int tail; // next enqueue position (producer)
  unsigned int count;

  uint64_t total_enqueued;
  uint64_t total_dropped;

  pthread_mutex_t mutex;    // protects head, tail, count
  pthread_cond_t not_empty; // signaled by producer

  bool shutdown; // consumer must exit when true
  bool initialized;
} circular_buffer_t;

// call before spawning any threads
cb_status_t cb_init(circular_buffer_t *cb);

// non blocking enqueue
cb_status_t cb_enqueue(circular_buffer_t *cb, char *json);

// blocking dequeue
cb_status_t cb_dequeue(circular_buffer_t *cb, char **out);

// lockless stats
cb_status_t cb_get_stats(circular_buffer_t *cb, cb_stats_t *stats);

// stats with mutex lock
cb_status_t cb_get_stats_locked(circular_buffer_t *cb, cb_stats_t *stats);

// sets shutdown flag
cb_status_t cb_shutdown(circular_buffer_t *cb);

// frees mutex, condvar, leftover strings
cb_status_t cb_destroy(circular_buffer_t *cb);

// returns strings for the status code
const char *cb_status_str(cb_status_t status);

#endif
