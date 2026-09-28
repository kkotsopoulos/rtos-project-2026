#include "circular_buffer.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline float _occupancy_pct(unsigned int count) {
  return (float)count * 100.0f / (float)BUFFER_CAPACITY;
}

cb_status_t cb_init(circular_buffer_t *cb) {
  if (cb == NULL) {
    return CB_ERR_NULL;
  }

  memset(cb, 0, sizeof(circular_buffer_t));

  // use errorcheck mutex
  pthread_mutexattr_t mutex_attr;
  int ret = pthread_mutexattr_init(&mutex_attr);
  if (ret != 0) {
    fprintf(stderr, "[cb_init] pthread_mutexattr_init failed: %s\n",
            strerror(ret));
    return CB_ERR_INVAL;
  }

  ret = pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK);
  if (ret != 0) {
    fprintf(stderr, "[cb_init] pthread_mutexattr_settype failed: %s\n",
            strerror(ret));
    pthread_mutexattr_destroy(&mutex_attr);
    return CB_ERR_INVAL;
  }

  ret = pthread_mutex_init(&cb->mutex, &mutex_attr);
  pthread_mutexattr_destroy(&mutex_attr);

  if (ret != 0) {
    fprintf(stderr, "[cb_init] pthread_mutex_init failed: %s\n", strerror(ret));
    return CB_ERR_INVAL;
  }

  // condition variable signaled when items are pushed
  ret = pthread_cond_init(&cb->not_empty, NULL);
  if (ret != 0) {
    fprintf(stderr, "[cb_init] pthread_cond_init failed: %s\n", strerror(ret));
    pthread_mutex_destroy(&cb->mutex);
    return CB_ERR_INVAL;
  }

  cb->initialized = true;
  return CB_OK;
}

cb_status_t cb_enqueue(circular_buffer_t *cb, char *json) {
  if (cb == NULL || json == NULL) {
    return CB_ERR_NULL;
  }

  int ret = pthread_mutex_lock(&cb->mutex);
  if (ret != 0) {
    fprintf(stderr, "[cb_enqueue] pthread_mutex_lock failed: %s\n",
            strerror(ret));
    return CB_ERR_INVAL;
  }

  if (!cb->initialized) {
    pthread_mutex_unlock(&cb->mutex);
    return CB_ERR_INVAL;
  }

  if (cb->shutdown) {
    pthread_mutex_unlock(&cb->mutex);
    return CB_ERR_SHUT;
  }

  // non-blocking: drop on full, caller must free rejected payload
  if (cb->count >= BUFFER_CAPACITY) {
    cb->total_dropped++;
    pthread_mutex_unlock(&cb->mutex);
    return CB_ERR_FULL;
  }

  // fast slot index wrap (BUFFER_CAPACITY is power of two)
  cb->slots[cb->tail & BUFFER_MASK] = json;
  cb->tail++;
  cb->count++;
  cb->total_enqueued++;

  // signal (not broadcast) because there's only one consumer
  ret = pthread_cond_signal(&cb->not_empty);
  if (ret != 0) {
    fprintf(stderr, "[cb_enqueue] pthread_cond_signal failed: %s\n",
            strerror(ret));
  }

  pthread_mutex_unlock(&cb->mutex);
  return CB_OK;
}

cb_status_t cb_dequeue(circular_buffer_t *cb, char **out) {
  if (cb == NULL || out == NULL) {
    return CB_ERR_NULL;
  }

  *out = NULL;

  int ret = pthread_mutex_lock(&cb->mutex);
  if (ret != 0) {
    fprintf(stderr, "[cb_dequeue] pthread_mutex_lock failed: %s\n",
            strerror(ret));
    return CB_ERR_INVAL;
  }

  if (!cb->initialized) {
    pthread_mutex_unlock(&cb->mutex);
    return CB_ERR_INVAL;
  }

  // while loop guards against spurious wakeups
  while (cb->count == 0 && !cb->shutdown) {
    ret = pthread_cond_wait(&cb->not_empty, &cb->mutex);
    if (ret != 0) {
      fprintf(stderr, "[cb_dequeue] pthread_cond_wait failed: %s\n",
              strerror(ret));
      pthread_mutex_unlock(&cb->mutex);
      return CB_ERR_INVAL;
    }
  }

  // shutdown + empty = exit
  if (cb->count == 0 && cb->shutdown) {
    pthread_mutex_unlock(&cb->mutex);
    return CB_ERR_SHUT;
  }

  // keep draining queued items even after shutdown
  *out = cb->slots[cb->head & BUFFER_MASK];
  cb->slots[cb->head & BUFFER_MASK] = NULL;
  cb->head++;
  cb->count--;

  pthread_mutex_unlock(&cb->mutex);

  // JSON parsing happens outside the lock to minimize contention
  return CB_OK;
}

cb_status_t cb_get_stats(circular_buffer_t *cb, cb_stats_t *stats) {
  if (cb == NULL || stats == NULL) {
    return CB_ERR_NULL;
  }

  // best-effort read without lock
  // slightly stale values are fine for monitoring
  stats->count = cb->count;
  stats->capacity = BUFFER_CAPACITY;
  stats->total_enqueued = cb->total_enqueued;
  stats->total_dropped = cb->total_dropped;
  stats->occupancy_pct = _occupancy_pct(cb->count);

  return CB_OK;
}

cb_status_t cb_get_stats_locked(circular_buffer_t *cb, cb_stats_t *stats) {
  if (cb == NULL || stats == NULL) {
    return CB_ERR_NULL;
  }

  int ret = pthread_mutex_lock(&cb->mutex);
  if (ret != 0) {
    return CB_ERR_INVAL;
  }

  stats->count = cb->count;
  stats->capacity = BUFFER_CAPACITY;
  stats->total_enqueued = cb->total_enqueued;
  stats->total_dropped = cb->total_dropped;
  stats->occupancy_pct = _occupancy_pct(cb->count);

  pthread_mutex_unlock(&cb->mutex);
  return CB_OK;
}

cb_status_t cb_shutdown(circular_buffer_t *cb) {
  if (cb == NULL) {
    return CB_ERR_NULL;
  }

  int ret = pthread_mutex_lock(&cb->mutex);
  if (ret != 0) {
    return CB_ERR_INVAL;
  }

  cb->shutdown = true;

  // wake up blocked consumers so they can drain and exit
  ret = pthread_cond_broadcast(&cb->not_empty);
  if (ret != 0) {
    fprintf(stderr, "[cb_shutdown] pthread_cond_broadcast failed: %s\n",
            strerror(ret));
  }

  pthread_mutex_unlock(&cb->mutex);
  return CB_OK;
}

cb_status_t cb_destroy(circular_buffer_t *cb) {
  if (cb == NULL) {
    return CB_ERR_NULL;
  }

  // must be called after pthread_join for all threads — no lock needed
  // free any unconsumed message payloads remaining in slots
  for (unsigned int i = 0; i < BUFFER_CAPACITY; i++) {
    if (cb->slots[i] != NULL) {
      free(cb->slots[i]);
      cb->slots[i] = NULL;
    }
  }

  fprintf(stderr,
          "[cb_destroy] Final stats: enqueued=%llu, dropped=%llu, "
          "remaining=%u\n",
          (unsigned long long)cb->total_enqueued,
          (unsigned long long)cb->total_dropped, cb->count);

  pthread_mutex_destroy(&cb->mutex);
  pthread_cond_destroy(&cb->not_empty);

  memset(cb, 0, sizeof(circular_buffer_t));

  return CB_OK;
}

const char *cb_status_str(cb_status_t status) {
  switch (status) {
  case CB_OK:
    return "CB_OK";
  case CB_ERR_FULL:
    return "CB_ERR_FULL (buffer full, message dropped)";
  case CB_ERR_NULL:
    return "CB_ERR_NULL (null pointer argument)";
  case CB_ERR_INVAL:
    return "CB_ERR_INVAL (not initialized or internal error)";
  case CB_ERR_SHUT:
    return "CB_ERR_SHUT (buffer shutdown, consumer exiting)";
  default:
    return "CB_UNKNOWN";
  }
}
