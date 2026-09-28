#include "monitor.h"
#include "circular_buffer.h"
#include "globals.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// macOS fallback for clock_nanosleep — only for compilation, real runs on Pi
#ifndef TIMER_ABSTIME
#define TIMER_ABSTIME 1
static int clock_nanosleep(clockid_t clk_id, int flags,
                           const struct timespec *req, struct timespec *rem) {
  (void)clk_id;
  (void)flags;
  (void)rem;
  return nanosleep(req, NULL);
}
#endif /* TIMER_ABSTIME */

static inline void timespec_add_one_second(struct timespec *ts) {
  ts->tv_sec += MONITOR_PERIOD_SEC;
  ts->tv_nsec += MONITOR_PERIOD_NSEC;

  if (ts->tv_nsec >= 1000000000L) {
    ts->tv_sec += 1;
    ts->tv_nsec -= 1000000000L;
  }
}

// absolute diff in nanoseconds (for jitter measurement)
static inline long long timespec_diff_ns(const struct timespec *a,
                                         const struct timespec *b) {
  long long diff = ((long long)(a->tv_sec - b->tv_sec) * 1000000000LL) +
                   ((long long)(a->tv_nsec - b->tv_nsec));
  return (diff < 0) ? -diff : diff;
}

// reads /proc/stat first line, returns 0 on success
static int read_proc_stat(cpu_jiffies_t *j) {
  FILE *fp = fopen("/proc/stat", "r");
  if (fp == NULL) {
    memset(j, 0, sizeof(cpu_jiffies_t));
    return -1;
  }

  char line[PROC_STAT_LINE_MAX];
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    memset(j, 0, sizeof(cpu_jiffies_t));
    return -1;
  }
  fclose(fp);

  unsigned long long user, nice, system, idle, iowait, irq, softirq, steal;

  int n = sscanf(line, "cpu  %llu %llu %llu %llu %llu %llu %llu %llu", &user,
                 &nice, &system, &idle, &iowait, &irq, &softirq, &steal);

  if (n < 4) {
    memset(j, 0, sizeof(cpu_jiffies_t));
    return -1;
  }

  j->user = user;
  j->nice = nice;
  j->system = system;
  j->idle = idle;
  j->iowait = (n >= 5) ? iowait : 0ULL;
  j->irq = (n >= 6) ? irq : 0ULL;
  j->softirq = (n >= 7) ? softirq : 0ULL;
  j->steal = (n >= 8) ? steal : 0ULL;

  // guest already in user
  j->total = j->user + j->nice + j->system + j->idle + j->iowait + j->irq +
             j->softirq + j->steal;

  return 0;
}

static float compute_cpu_pct(const cpu_jiffies_t *prev,
                             const cpu_jiffies_t *curr) {
  unsigned long long delta_total = curr->total - prev->total;
  unsigned long long delta_idle = curr->idle - prev->idle;

  if (delta_total == 0ULL) {
    return 0.0f;
  }

  unsigned long long delta_busy = delta_total - delta_idle;
  return (float)delta_busy * 100.0f / (float)delta_total;
}

// opens log in append mode, writes CSV header if empty
static FILE *open_log_file(void) {
  FILE *fp = fopen(METRICS_LOG_FILE, "a");
  if (fp == NULL) {
    fprintf(stderr, "[monitor] Cannot open %s: %s\n", METRICS_LOG_FILE,
            strerror(errno));
    return NULL;
  }

  if (ftell(fp) == 0) {
    fprintf(fp, "Seconds,Nanoseconds,Commit_Count,Identity_Count,"
                "Account_Count,Info_Count,Buffer_Occupancy_Pct,CPU_Pct\n");
    fflush(fp);
    fprintf(stderr, "[monitor] New log file created: %s\n", METRICS_LOG_FILE);
  } else {
    fprintf(stderr, "[monitor] Appending to existing log: %s\n",
            METRICS_LOG_FILE);
  }

  return fp;
}

void *monitor_thread_fn(void *arg) {
  (void)arg;

  fprintf(stderr, "[monitor] Thread started (period=1s, CLOCK_MONOTONIC)\n");

  FILE *log_fp = open_log_file();
  if (log_fp == NULL) {
    fprintf(stderr, "[monitor] Fatal: cannot open log file. Thread exiting.\n");
    return NULL;
  }

  // baseline /proc/stat read for CPU% delta
  cpu_jiffies_t cpu_prev;
  cpu_jiffies_t cpu_curr;
  bool cpu_valid = (read_proc_stat(&cpu_prev) == 0);

  if (!cpu_valid) {
    fprintf(stderr, "[monitor] Warning: /proc/stat not available "
                    "(macOS?). CPU%% will be 0.0.\n");
    memset(&cpu_prev, 0, sizeof(cpu_prev));
  }

  // first wakeup at now + 1s (CLOCK_MONOTONIC, immune to NTP/DST)
  struct timespec t_next;
  clock_gettime(CLOCK_MONOTONIC, &t_next);
  timespec_add_one_second(&t_next);

  uint64_t cycle = 0;

  while (!g_shutdown) {

    // absolute sleep — no drift accumulation
    int ret = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &t_next, NULL);
    if (ret == EINTR) {
      continue; // interrupted by signal, re-check g_shutdown
    }
    if (ret != 0) {
      fprintf(stderr, "[monitor] clock_nanosleep error: %s\n", strerror(ret));
      break;
    }

    struct timespec t_real;
    struct timespec t_actual;

    clock_gettime(CLOCK_REALTIME, &t_real);    // for CSV timestamp
    clock_gettime(CLOCK_MONOTONIC, &t_actual); // for jitter

    long long jitter_ns = timespec_diff_ns(&t_actual, &t_next);

    // CPU%
    float cpu_pct = 0.0f;

    if (read_proc_stat(&cpu_curr) == 0) {
      if (cpu_valid) {
        cpu_pct = compute_cpu_pct(&cpu_prev, &cpu_curr);
      }
      cpu_prev = cpu_curr;
      cpu_valid = true;
    }

    // snapshot counters and zero the window (locked internally)
    uint64_t counts[MSG_KIND_COUNT];
    counters_snapshot_and_reset(&g_app.counters, counts);

    // buffer occupancy (separate lock from counters, no deadlock risk)
    cb_stats_t buf_stats;
    cb_get_stats_locked(&g_app.buffer, &buf_stats);

    // write CSV line and flush immediately for crash safety
    int written = fprintf(log_fp, "%ld,%ld,%llu,%llu,%llu,%llu,%.2f,%.2f\n",
                          (long)t_real.tv_sec, (long)t_real.tv_nsec,
                          (unsigned long long)counts[MSG_KIND_COMMIT],
                          (unsigned long long)counts[MSG_KIND_IDENTITY],
                          (unsigned long long)counts[MSG_KIND_ACCOUNT],
                          (unsigned long long)counts[MSG_KIND_INFO],
                          (double)buf_stats.occupancy_pct, (double)cpu_pct);

    if (written < 0) {
      fprintf(stderr, "[monitor] fprintf to log failed: %s\n", strerror(errno));
    } else {
      fflush(log_fp);
    }

#ifdef DEBUG
    fprintf(stderr,
            "[monitor] cycle=%llu | ts=%ld.%09ld | "
            "commit=%llu id=%llu acc=%llu info=%llu | "
            "buf=%.1f%% | cpu=%.2f%% | jitter=%lld ns\n",
            (unsigned long long)cycle, (long)t_real.tv_sec,
            (long)t_real.tv_nsec, (unsigned long long)counts[MSG_KIND_COMMIT],
            (unsigned long long)counts[MSG_KIND_IDENTITY],
            (unsigned long long)counts[MSG_KIND_ACCOUNT],
            (unsigned long long)counts[MSG_KIND_INFO],
            (double)buf_stats.occupancy_pct, (double)cpu_pct, jitter_ns);
#else
    (void)jitter_ns;
#endif

    cycle++;

    // advance deadline by 1s from the ideal time (not actual), prevents drift
    timespec_add_one_second(&t_next);
  }

  fprintf(stderr,
          "[monitor] Shutting down after %llu cycles. "
          "Log: %s\n",
          (unsigned long long)cycle, METRICS_LOG_FILE);

  if (fflush(log_fp) != 0) {
    fprintf(stderr, "[monitor] fflush warning: %s\n", strerror(errno));
  }
  if (fclose(log_fp) != 0) {
    fprintf(stderr, "[monitor] fclose warning: %s\n", strerror(errno));
  }

  fprintf(stderr, "[monitor] Thread exited cleanly\n");
  return NULL;
}
