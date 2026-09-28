#include "consumer.h"
#include "circular_buffer.h"
#include "globals.h"

// cJSON path
#ifdef __has_include
#if __has_include(<cjson/cJSON.h>)
#include <cjson/cJSON.h>
#else
#include <cJSON.h>
#endif
#else
#include <cjson/cJSON.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// kind lookup
typedef struct {
  const char *str;
  msg_kind_t kind;
} kind_entry_t;

static const kind_entry_t KIND_TABLE[] = {
    {KIND_STR_COMMIT, MSG_KIND_COMMIT},
    {KIND_STR_IDENTITY, MSG_KIND_IDENTITY},
    {KIND_STR_ACCOUNT, MSG_KIND_ACCOUNT},
    {KIND_STR_INFO, MSG_KIND_INFO},
};

#define KIND_TABLE_SIZE (sizeof(KIND_TABLE) / sizeof(KIND_TABLE[0]))

// map kind string to enum, returns UNKNOWN if unmatched
static msg_kind_t parse_kind(const char *kind_str) {
  for (size_t i = 0; i < KIND_TABLE_SIZE; i++) {
    if (strcmp(kind_str, KIND_TABLE[i].str) == 0) {
      return KIND_TABLE[i].kind;
    }
  }
  return MSG_KIND_UNKNOWN;
}

void *consumer_thread_fn(void *arg) {
  (void)arg;

  fprintf(stderr, "[consumer] Thread started\n");

  uint64_t local_processed = 0;
  uint64_t local_parse_err = 0;
  uint64_t local_unknown = 0;

  // drain queue until shutdown and buffer is completely empty
  while (1) {

    char *json_str = NULL;
    cb_status_t st = cb_dequeue(&g_app.buffer, &json_str);

    if (st == CB_ERR_SHUT) {
      break; // clean exit once drained
    }

    if (st != CB_OK || json_str == NULL) {
      fprintf(stderr, "[consumer] cb_dequeue unexpected: %s\n",
              cb_status_str(st));
      continue;
    }

    cJSON *root = cJSON_Parse(json_str);

    // free raw string right after parse — cJSON made its own copy
    free(json_str);
    json_str = NULL;

    if (root == NULL) {
      local_parse_err++;
      continue;
    }

    // extract event type from payload
    const cJSON *kind_item = cJSON_GetObjectItemCaseSensitive(root, "kind");

    if (!cJSON_IsString(kind_item) || kind_item->valuestring == NULL) {
      local_unknown++;
      cJSON_Delete(root);
      continue;
    }

    msg_kind_t kind = parse_kind(kind_item->valuestring);

    if (kind == MSG_KIND_UNKNOWN) {
      local_unknown++;
      // manual lock needed as counters_increment only handles known kinds
      pthread_mutex_lock(&g_app.counters.counters_mutex);
      g_app.counters.unknown_count++;
      pthread_mutex_unlock(&g_app.counters.counters_mutex);
    } else {
      counters_increment(&g_app.counters, kind);
    }

    local_processed++;

    cJSON_Delete(root);
  }

  fprintf(stderr,
          "[consumer] Thread exiting. "
          "processed=%llu, parse_errors=%llu, unknown_kind=%llu\n",
          (unsigned long long)local_processed,
          (unsigned long long)local_parse_err,
          (unsigned long long)local_unknown);

  return NULL;
}
