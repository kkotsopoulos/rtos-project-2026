#ifndef MONITOR_H
#define MONITOR_H

#include <stdio.h>
#include <time.h>

#define METRICS_LOG_FILE "metrics_log.txt"

#define MONITOR_PERIOD_SEC 1L
#define MONITOR_PERIOD_NSEC 0L

#define MONITOR_CSV_LINE_MAX 256
#define PROC_STAT_LINE_MAX 256

// CPU jiffies from /proc/stat for delta-based CPU% calculation
typedef struct {
  unsigned long long user;
  unsigned long long nice;
  unsigned long long system;
  unsigned long long idle;
  unsigned long long iowait;
  unsigned long long irq;
  unsigned long long softirq;
  unsigned long long steal;
  unsigned long long total; // sum of all above
} cpu_jiffies_t;

// periodic 1s logger thread
// uses clock_nanosleep for zero-drift wakeup
// reads /proc/stat, snapshots counters + buffer stats, writes CSV line
// exits on g_shutdown == 1
void *monitor_thread_fn(void *arg);

#endif
