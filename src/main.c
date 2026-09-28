#include "circular_buffer.h"
#include "consumer.h"
#include "globals.h"
#include "monitor.h"
#include "producer.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define APP_VERSION "1.0.0"
#define APP_NAME "RTOS Firehose Monitor"

// 86400s = 24h, set to 0 for unlimited
#define MAX_RUN_DURATION_SEC 86400L

// only async-signal-safe ops here
static void signal_handler(int signum) {
  (void)signum;
  g_shutdown = 1;
}

// install handlers for clean exit on SIGINT / SIGTERM
static int setup_signals(void) {
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = signal_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESTART; // restart interrupted syscalls

  if (sigaction(SIGINT, &sa, NULL) != 0) {
    perror("sigaction(SIGINT)");
    return -1;
  }
  if (sigaction(SIGTERM, &sa, NULL) != 0) {
    perror("sigaction(SIGTERM)");
    return -1;
  }

  signal(SIGPIPE, SIG_IGN); // websocket disconnect shouldn't crash us

  return 0;
}

static void print_startup_info(void) {
  fprintf(stderr,
          "  %s v%s\n"
          "  RTOS Final Project 2026\n"
          "  Endpoint : wss://%s%s\n"
          "  Buffer   : %u slots\n"
          "  Duration : %ld s (%.1f h)%s\n",
          APP_NAME, APP_VERSION, LWS_HOST, LWS_PATH, BUFFER_CAPACITY,
          (long)MAX_RUN_DURATION_SEC, (double)MAX_RUN_DURATION_SEC / 3600.0,
          (MAX_RUN_DURATION_SEC == 0) ? " [unlimited]" : "");
}

int main(void) {
  int exit_code = EXIT_SUCCESS;

  print_startup_info();

  struct timespec t_start;
  clock_gettime(CLOCK_MONOTONIC, &t_start);

  if (setup_signals() != 0) {
    fprintf(stderr, "[main] Signal setup failed\n");
    return EXIT_FAILURE;
  }

  // initialize shared queue and counters before spawning threads
  cb_status_t cb_ret = cb_init(&g_app.buffer);
  if (cb_ret != CB_OK) {
    fprintf(stderr, "[main] cb_init failed: %s\n", cb_status_str(cb_ret));
    return EXIT_FAILURE;
  }
  fprintf(stderr, "[main] Circular buffer initialized (%u slots)\n",
          BUFFER_CAPACITY);

  if (counters_init(&g_app.counters) != 0) {
    fprintf(stderr, "[main] counters_init failed\n");
    cb_destroy(&g_app.buffer);
    return EXIT_FAILURE;
  }
  fprintf(stderr, "[main] Message counters initialized\n");

  // truncate old log and write CSV header before threads start
  // monitor thread opens in append("a") mode after this
  {
    FILE *fp = fopen(METRICS_LOG_FILE, "w");
    if (fp != NULL) {
      fprintf(fp, "Seconds,Nanoseconds,Commit_Count,Identity_Count,"
                  "Account_Count,Info_Count,Buffer_Occupancy_Pct,CPU_Pct\n");
      fclose(fp);
      fprintf(stderr, "[main] Metrics log initialized: %s\n", METRICS_LOG_FILE);
    } else {
      fprintf(stderr, "[main] WARNING: Could not create %s: %s\n",
              METRICS_LOG_FILE, strerror(errno));
    }
  }

  // thread start order matters: consumer -> monitor -> producer
  pthread_t consumer_tid;
  pthread_t monitor_tid;
  pthread_t producer_tid;

  int ret = pthread_create(&consumer_tid, NULL, consumer_thread_fn, NULL);
  if (ret != 0) {
    fprintf(stderr, "[main] pthread_create(consumer): %s\n", strerror(ret));
    exit_code = EXIT_FAILURE;
    goto cleanup_counters;
  }
  fprintf(stderr, "[main] Consumer thread started\n");

  ret = pthread_create(&monitor_tid, NULL, monitor_thread_fn, NULL);
  if (ret != 0) {
    fprintf(stderr, "[main] pthread_create(monitor): %s\n", strerror(ret));
    cb_shutdown(&g_app.buffer);
    pthread_join(consumer_tid, NULL);
    exit_code = EXIT_FAILURE;
    goto cleanup_counters;
  }
  fprintf(stderr, "[main] Monitor thread started (log: %s)\n",
          METRICS_LOG_FILE);

  ret = pthread_create(&producer_tid, NULL, producer_thread_fn, NULL);
  if (ret != 0) {
    fprintf(stderr, "[main] pthread_create(producer): %s\n", strerror(ret));
    // unwind already created threads on failure
    g_shutdown = 1;
    cb_shutdown(&g_app.buffer);
    pthread_join(monitor_tid, NULL);
    pthread_join(consumer_tid, NULL);
    exit_code = EXIT_FAILURE;
    goto cleanup_counters;
  }
  fprintf(stderr, "[main] Producer thread started\n");

  // main loop: sleep 1s, check for timeout or signal
  fprintf(stderr,
          "[main] System running. Target duration: %ld s (%.1f h)...\n\n",
          (long)MAX_RUN_DURATION_SEC, (double)MAX_RUN_DURATION_SEC / 3600.0);

  while (!g_shutdown) {
    sleep(1);

    if (g_shutdown) {
      break;
    }

    if (MAX_RUN_DURATION_SEC > 0) {
      struct timespec t_now;
      clock_gettime(CLOCK_MONOTONIC, &t_now);
      long elapsed_sec = (long)(t_now.tv_sec - t_start.tv_sec);

      // stop automatically once duration limit is reached
      if (elapsed_sec >= MAX_RUN_DURATION_SEC) {
        fprintf(stderr,
                "\n[main] Target run duration reached (%ld s / %ld s).\n"
                "[main] Initiating automatic graceful shutdown...\n",
                elapsed_sec, (long)MAX_RUN_DURATION_SEC);
        g_shutdown = 1;
        break;
      }
    }
  }

  fprintf(
      stderr,
      "\n[main] Shutdown initiated. Stopping threads and finalizing logs...\n");

  // graceful shutdown: cb_shutdown wakes consumer even if producer is slow
  cb_shutdown(&g_app.buffer);

  // join in reverse order
  fprintf(stderr, "[main] Joining producer thread...\n");
  pthread_join(producer_tid, NULL);
  fprintf(stderr, "[main] Producer joined.\n");

  fprintf(stderr, "[main] Joining consumer thread...\n");
  pthread_join(consumer_tid, NULL);
  fprintf(stderr, "[main] Consumer joined.\n");

  fprintf(stderr, "[main] Joining monitor thread...\n");
  pthread_join(monitor_tid, NULL);
  fprintf(stderr, "[main] Monitor joined.\n");

cleanup_counters:
  // release sync primitives and allocated buffer memory
  counters_destroy(&g_app.counters);
  cb_destroy(&g_app.buffer);

  fprintf(stderr, "[main] Cleanup complete. Exiting.\n");
  return exit_code;
}
