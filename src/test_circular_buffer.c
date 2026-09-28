#include "circular_buffer.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TEST_PASS "[PASS]"
#define TEST_FAIL "[FAIL]"
#define TEST_INFO "[INFO]"

// prints only on failure, silently counts passes
#define CHECK(expr)                                                            \
  do {                                                                         \
    if (!(expr)) {                                                             \
      fprintf(stderr, "%s %s:%d: assertion failed: %s\n", TEST_FAIL, __FILE__, \
              __LINE__, #expr);                                                \
      failures++;                                                              \
    } else {                                                                   \
      passes++;                                                                \
    }                                                                          \
  } while (0)

static int passes = 0;
static int failures = 0;

// builds a fake Jetstream JSON message on the heap
static char *make_json(const char *kind, int id) {
  char *buf = malloc(256);
  if (!buf) {
    perror("malloc");
    exit(EXIT_FAILURE);
  }
  snprintf(buf, 256, "{\"kind\":\"%s\",\"seq\":%d,\"did\":\"did:plc:test%d\"}",
           kind, id, id);
  return buf;
}

static void test_init_destroy(void) {
  printf("\nTEST 1: Init / Destroy \n");
  circular_buffer_t cb;

  CHECK(cb_init(NULL) == CB_ERR_NULL); // NULL arg must be rejected
  CHECK(cb_init(&cb) == CB_OK);
  // verify initial state: empty, zeroed pointers, not shutdown
  CHECK(cb.initialized == true);
  CHECK(cb.count == 0);
  CHECK(cb.head == 0);
  CHECK(cb.tail == 0);
  CHECK(cb.shutdown == false);

  CHECK(cb_destroy(&cb) == CB_OK);
  printf("%s Init/Destroy\n", TEST_PASS);
}

static void test_single_thread(void) {
  printf("\n TEST 2: Single-thread enqueue/dequeue \n");
  circular_buffer_t cb;
  char *out = NULL;

  cb_init(&cb);

  char *msg = make_json("commit", 1);
  CHECK(cb_enqueue(&cb, msg) == CB_OK);
  CHECK(cb.count == 1);

  printf("%s Single-thread enqueue OK (count=%u)\n", TEST_PASS, cb.count);

  // drain queued data after shutdown without losing items
  cb_shutdown(&cb);
  cb_status_t st = cb_dequeue(&cb, &out);
  CHECK(st == CB_OK && out != NULL);
  if (out) {
    free(out);
    out = NULL;
  }

  // once empty, subsequent dequeues must return CB_ERR_SHUT
  st = cb_dequeue(&cb, &out);
  CHECK(st == CB_ERR_SHUT);

  cb_destroy(&cb);
  printf("%s Single-thread drain after shutdown\n", TEST_PASS);
}

static void test_overflow(void) {
  printf("\n TEST 3: Overflow / CB_ERR_FULL \n");
  circular_buffer_t cb;
  cb_init(&cb);

  // fill buffer to capacity
  for (unsigned int i = 0; i < BUFFER_CAPACITY; i++) {
    char *msg = make_json("commit", (int)i);
    cb_status_t st = cb_enqueue(&cb, msg);
    if (st != CB_OK) {
      fprintf(stderr, "[FAIL] enqueue failed at i=%u: %s\n", i,
              cb_status_str(st));
      free(msg);
      failures++;
    } else {
      passes++;
    }
  }

  CHECK(cb.count == BUFFER_CAPACITY);
  printf("%s Buffer full (%u/%u)\n", TEST_PASS, cb.count, BUFFER_CAPACITY);

  // next enqueue must fail
  char *overflow_msg = make_json("commit", 9999);
  cb_status_t st = cb_enqueue(&cb, overflow_msg);
  CHECK(st == CB_ERR_FULL);
  CHECK(cb.total_dropped == 1);
  free(overflow_msg); // caller owns the pointer on CB_ERR_FULL
  printf("%s Overflow -> CB_ERR_FULL, total_dropped=1\n", TEST_PASS);

  // drain buffer before cleanup to prevent memory leaks
  cb_shutdown(&cb);
  char *out = NULL;
  while (cb_dequeue(&cb, &out) == CB_OK) {
    free(out);
    out = NULL;
  }
  cb_destroy(&cb);
}

static void test_fifo_order(void) {
  printf("\nTEST 4: FIFO order \n");
  circular_buffer_t cb;
  cb_init(&cb);

  // enqueue sequential messages with monotonic seq ids
  const int N = 10;
  for (int i = 0; i < N; i++) {
    char *msg = make_json("commit", i);
    cb_enqueue(&cb, msg);
  }

  cb_shutdown(&cb);

  // dequeue all and check seq field matches insertion order
  for (int i = 0; i < N; i++) {
    char *out = NULL;
    CHECK(cb_dequeue(&cb, &out) == CB_OK);
    if (out) {
      char expected_seq[32];
      snprintf(expected_seq, sizeof(expected_seq), "\"seq\":%d", i);
      int found = (strstr(out, expected_seq) != NULL);
      CHECK(found == 1);
      if (!found) {
        fprintf(stderr, "  Expected seq=%d, got: %s\n", i, out);
      }
      free(out);
    }
  }

  cb_destroy(&cb);
  printf("%s FIFO order validated (%d messages)\n", TEST_PASS, N);
}

static void test_stats(void) {
  printf("\n TEST 5: Statistics / Occupancy \n");
  circular_buffer_t cb;
  cb_stats_t stats;
  cb_init(&cb);

  // empty buffer should report 0%
  cb_get_stats_locked(&cb, &stats);
  CHECK(stats.count == 0);
  CHECK(stats.capacity == BUFFER_CAPACITY);
  CHECK(stats.occupancy_pct >= 0.0f && stats.occupancy_pct < 0.01f);

  for (unsigned int i = 0; i < BUFFER_CAPACITY / 2; i++) {
    char *msg = make_json("identity", (int)i);
    cb_enqueue(&cb, msg);
  }

  cb_get_stats_locked(&cb, &stats);
  CHECK(stats.count == BUFFER_CAPACITY / 2);
  CHECK(stats.occupancy_pct >= 49.5f && stats.occupancy_pct <= 50.5f);
  printf("%s Occupancy at 50%%: %.2f%%\n", TEST_PASS, stats.occupancy_pct);

  cb_shutdown(&cb);
  char *out = NULL;
  while (cb_dequeue(&cb, &out) == CB_OK) {
    free(out);
    out = NULL;
  }
  cb_destroy(&cb);
}

#define STRESS_MESSAGES 10000

typedef struct {
  circular_buffer_t *cb;
  int kind_idx;
} producer_args_t;

typedef struct {
  circular_buffer_t *cb;
  unsigned int received;
  unsigned int errors;
} consumer_args_t;

static const char *kinds[] = {"commit", "identity", "account", "info"};

// producer worker: generates messages and discards on CB_ERR_FULL
static void *stress_producer(void *arg) {
  producer_args_t *args = (producer_args_t *)arg;
  int dropped = 0;

  for (int i = 0; i < STRESS_MESSAGES; i++) {
    char *msg = make_json(kinds[args->kind_idx], i);
    cb_status_t st = cb_enqueue(args->cb, msg);
    if (st == CB_ERR_FULL) {
      free(msg);
      dropped++;
    } else if (st != CB_OK) {
      free(msg);
      break;
    }
  }

  fprintf(stderr, "%s Producer[%s] done. Dropped: %d/%d\n", TEST_INFO,
          kinds[args->kind_idx], dropped, STRESS_MESSAGES);
  return NULL;
}

// consumer worker: drains until buffer is shut down
static void *stress_consumer(void *arg) {
  consumer_args_t *args = (consumer_args_t *)arg;
  char *out = NULL;

  while (1) {
    cb_status_t st = cb_dequeue(args->cb, &out);
    if (st == CB_ERR_SHUT) {
      break;
    } else if (st == CB_OK && out != NULL) {
      args->received++;
      free(out);
      out = NULL;
    } else {
      args->errors++;
    }
  }

  return NULL;
}

static void test_multithreaded_stress(void) {
  printf("\n TEST 6: Multithreaded Stress (4 producers, 1 consumer)\n");

  circular_buffer_t cb;
  cb_init(&cb);

  pthread_t producers[4];
  pthread_t consumer_tid;
  producer_args_t prod_args[4];
  consumer_args_t cons_args = {.cb = &cb, .received = 0, .errors = 0};

  // start consumer first so it's ready before producers flood the buffer
  pthread_create(&consumer_tid, NULL, stress_consumer, &cons_args);

  // spawn 4 concurrent producers, each targeting a distinct event kind
  for (int i = 0; i < 4; i++) {
    prod_args[i].cb = &cb;
    prod_args[i].kind_idx = i;
    pthread_create(&producers[i], NULL, stress_producer, &prod_args[i]);
  }

  for (int i = 0; i < 4; i++) {
    pthread_join(producers[i], NULL);
  }

  // all producers done, tell consumer to finish draining
  cb_shutdown(&cb);
  pthread_join(consumer_tid, NULL);

  cb_stats_t stats;
  cb_get_stats(&cb, &stats);

  printf("%s Stress test complete:\n", TEST_PASS);
  printf("      Total enqueued : %llu\n",
         (unsigned long long)stats.total_enqueued);
  printf("      Total dropped  : %llu\n",
         (unsigned long long)stats.total_dropped);
  printf("      Consumer got   : %u\n", cons_args.received);
  printf("      Consumer errors: %u\n", cons_args.errors);

  // conservation check: total produced must equal enqueued + dropped
  CHECK((stats.total_enqueued + stats.total_dropped) ==
        (uint64_t)(4 * STRESS_MESSAGES));
  CHECK(cons_args.received == (unsigned int)stats.total_enqueued);
  CHECK(cons_args.errors == 0);

  cb_destroy(&cb);
}

int main(void) {
  printf("  Circular Buffer Unit Tests\n");
  printf("  RTOS Final Project 2026\n");

  test_init_destroy();
  test_single_thread();
  test_overflow();
  test_fifo_order();
  test_stats();
  test_multithreaded_stress();

  printf("  Results: %d passed, %d failed\n", passes, failures);

  return (failures > 0) ? EXIT_FAILURE : EXIT_SUCCESS;
}
