#include "producer.h"
#include "circular_buffer.h"
#include "globals.h"

#include <libwebsockets.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// per-session state (static because there's only one connection)
typedef struct {
  char *assemble_buf; // heap buffer for partial frame reassembly
  size_t assemble_len;
  size_t assemble_cap;

  uint64_t frames_received;
  uint64_t frames_dropped;
  uint64_t bytes_received;

  bool connected;
} producer_session_t;

static producer_session_t s_session;

static struct lws_context *s_lws_context = NULL;
static struct lws *s_wsi = NULL;

// watchdog: prevent zombie connections (socket open, no data)
#define WATCHDOG_IDLE_TIMEOUT_SEC 10
static time_t s_last_rx_time = 0;

static bool assemble_buf_ensure(producer_session_t *sess, size_t needed) {
  if (needed <= sess->assemble_cap) {
    return true;
  }

  // double capacity, clamp to max frame limit
  size_t new_cap = sess->assemble_cap * 2;
  if (new_cap < needed) {
    new_cap = needed;
  }
  if (new_cap > LWS_MAX_FRAME_SIZE) {
    fprintf(stderr,
            "[producer] Frame too large (%zu bytes > %d max), discarding\n",
            needed, LWS_MAX_FRAME_SIZE);
    return false;
  }

  char *p = realloc(sess->assemble_buf, new_cap);
  if (p == NULL) {
    fprintf(stderr, "[producer] realloc failed for assemble_buf\n");
    return false;
  }
  sess->assemble_buf = p;
  sess->assemble_cap = new_cap;
  return true;
}

static inline void assemble_buf_reset(producer_session_t *sess) {
  sess->assemble_len = 0;
}

// enqueues a complete assembled frame into the circular buffer
static void producer_enqueue_frame(producer_session_t *sess) {
  if (sess->assemble_len == 0) {
    return;
  }

  // duplicate buffer so queue owns an independent copy
  char *json_copy = malloc(sess->assemble_len + 1);
  if (json_copy == NULL) {
    fprintf(stderr, "[producer] malloc failed for json_copy (%zu bytes)\n",
            sess->assemble_len + 1);
    sess->frames_dropped++;
    assemble_buf_reset(sess);
    return;
  }

  memcpy(json_copy, sess->assemble_buf, sess->assemble_len);
  json_copy[sess->assemble_len] = '\0';

  cb_status_t st = cb_enqueue(&g_app.buffer, json_copy);

  if (st == CB_ERR_FULL) {
    // buffer full — we still own the pointer, must free
    free(json_copy);
    sess->frames_dropped++;
  } else if (st == CB_ERR_SHUT) {
    free(json_copy);
  } else if (st != CB_OK) {
    fprintf(stderr, "[producer] cb_enqueue unexpected: %s\n",
            cb_status_str(st));
    free(json_copy);
    sess->frames_dropped++;
  } else {
    // CB_OK: buffer owns the pointer now
    sess->frames_received++;
    sess->bytes_received += sess->assemble_len;
  }

  assemble_buf_reset(sess);
}

// lws callback — runs on producer thread only (single-threaded lws context)
static int firehose_callback(struct lws *wsi, enum lws_callback_reasons reason,
                             void *user, void *in, size_t len) {
  (void)user;

  switch (reason) {

  case LWS_CALLBACK_CLIENT_ESTABLISHED:
    fprintf(stderr, "[producer] WebSocket connected to %s\n", LWS_HOST);
    s_session.connected = true;
    s_last_rx_time = time(NULL); // init watchdog
    assemble_buf_reset(&s_session);
    break;

  case LWS_CALLBACK_CLIENT_RECEIVE: {
    const char *payload = (const char *)in;

    // reassemble fragmented frames into contiguous JSON
    if (lws_is_first_fragment(wsi)) {
      assemble_buf_reset(&s_session);
    }

    size_t new_total = s_session.assemble_len + len;

    if (!assemble_buf_ensure(&s_session, new_total)) {
      assemble_buf_reset(&s_session);
      s_session.frames_dropped++;
      break;
    }

    memcpy(s_session.assemble_buf + s_session.assemble_len, payload, len);
    s_session.assemble_len += len;

    if (lws_is_final_fragment(wsi)) {
      s_last_rx_time = time(NULL); // reset watchdog
      producer_enqueue_frame(&s_session);
    }
    break;
  }

  case LWS_CALLBACK_CLIENT_CONNECTION_ERROR: {
    const char *err_str = (in != NULL) ? (const char *)in : "unknown";
    fprintf(stderr, "[producer] Connection error: %s\n", err_str);
    s_session.connected = false;
    s_wsi = NULL;
    break;
  }

  case LWS_CALLBACK_CLIENT_CLOSED:
    fprintf(stderr, "[producer] WebSocket connection closed\n");
    s_session.connected = false;
    s_wsi = NULL;
    break;

  case LWS_CALLBACK_WSI_DESTROY:
    // handle torn down by lws
    if (wsi == s_wsi) {
      s_wsi = NULL;
      s_session.connected = false;
    }
    break;

  default:
    break;
  }

  return 0;
}

static const struct lws_protocols s_protocols[] = {
    {
        .name = LWS_PROTO_NAME,
        .callback = firehose_callback,
        .per_session_data_size = 0,
        .rx_buffer_size = 65536,
    },
    LWS_PROTOCOL_LIST_TERM};

static struct lws_context *producer_create_context(void) {
  struct lws_context_creation_info info;
  memset(&info, 0, sizeof(info));

  info.port = CONTEXT_PORT_NO_LISTEN; // client only, no server socket
  info.protocols = s_protocols;
  info.gid = -1;
  info.uid = -1;
  info.pt_serv_buf_size = 65536;

  // force IPv4
  info.options =
      LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT | LWS_SERVER_OPTION_DISABLE_IPV6;

  struct lws_context *ctx = lws_create_context(&info);
  if (ctx == NULL) {
    fprintf(stderr, "[producer] lws_create_context failed\n");
  }
  return ctx;
}

static struct lws *producer_connect(struct lws_context *ctx) {
  struct lws_client_connect_info ccinfo;
  memset(&ccinfo, 0, sizeof(ccinfo));

  ccinfo.context = ctx;
  ccinfo.address = LWS_HOST;
  ccinfo.port = LWS_PORT;
  ccinfo.path = LWS_PATH;
  ccinfo.host = LWS_HOST;
  ccinfo.origin = LWS_HOST;
  ccinfo.protocol = LWS_PROTO_NAME;

  // permissive TLS for lab / proxy setups
  ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED |
                          LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

  struct lws *wsi = lws_client_connect_via_info(&ccinfo);
  if (wsi == NULL) {
    fprintf(stderr, "[producer] lws_client_connect_via_info failed\n");
  }
  return wsi;
}

// interruptible sleep that checks g_shutdown every 100ms
static void producer_sleep_ms(unsigned int ms) {
  const unsigned int step_ms = 100;
  struct timespec ts = {0, (long)(step_ms * 1000000L)};

  while (ms > 0 && !g_shutdown) {
    unsigned int sleep_now = (ms < step_ms) ? ms : step_ms;
    ts.tv_nsec = (long)(sleep_now * 1000000L);
    nanosleep(&ts, NULL);
    ms -= sleep_now;
  }
}

void *producer_thread_fn(void *arg) {
  (void)arg;

  fprintf(stderr, "[producer] Thread started\n");

  memset(&s_session, 0, sizeof(s_session));

  s_session.assemble_cap = 4096;
  s_session.assemble_buf = malloc(s_session.assemble_cap);
  if (s_session.assemble_buf == NULL) {
    fprintf(stderr, "[producer] Initial malloc failed — thread exiting\n");
    return NULL;
  }

  s_lws_context = producer_create_context();
  if (s_lws_context == NULL) {
    free(s_session.assemble_buf);
    return NULL;
  }

  unsigned int reconnect_delay_ms = RECONNECT_DELAY_MS_INIT;

  // main loop: connect -> service -> reconnect on failure
  while (!g_shutdown) {

    fprintf(stderr, "[producer] Connecting to wss://%s%s ...\n", LWS_HOST,
            LWS_PATH);

    s_wsi = producer_connect(s_lws_context);
    if (s_wsi == NULL) {
      fprintf(stderr,
              "[producer] Connect failed, retrying in %ums\n",
              reconnect_delay_ms);
      producer_sleep_ms(reconnect_delay_ms);
      reconnect_delay_ms *= 2;
      if (reconnect_delay_ms > RECONNECT_DELAY_MS_MAX) {
        reconnect_delay_ms = RECONNECT_DELAY_MS_MAX;
      }
      continue;
    }

    // don't reset backoff here — WSI is allocated but connection is async;
    // reset only after confirmed establishment (see below)
    bool established = false;

    // lws service loop — s_wsi is set to NULL by callbacks on disconnect
    while (!g_shutdown && s_wsi != NULL) {
      int ret = lws_service(s_lws_context, LWS_SERVICE_TIMEOUT_MS); // pump event loop
      if (ret < 0) {
        fprintf(stderr, "[producer] lws_service error: %d\n", ret);
        break;
      }

      // track actual connection establishment
      if (s_session.connected) {
        established = true;
      }

      // watchdog: force reconnect if no data for 10s
      if (s_session.connected && s_last_rx_time > 0) {
        time_t now = time(NULL);
        double idle_sec = difftime(now, s_last_rx_time);
        if (idle_sec >= (double)WATCHDOG_IDLE_TIMEOUT_SEC) {
          fprintf(stderr,
                  "[producer] Watchdog timeout (%.0fs). Forcing reconnect...\n",
                  idle_sec);
          // kill the wsi — triggers CLOSED callback -> backoff reconnect
          lws_set_timeout(s_wsi, PENDING_TIMEOUT_USER_REASON_BASE,
                          LWS_TO_KILL_SYNC);
          break;
        }
      }
    }

    // reset backoff only after a successfully established connection
    if (established) {
      reconnect_delay_ms = RECONNECT_DELAY_MS_INIT;
    }

    // connection dropped (not shutdown) -> reconnect with backoff
    if (!g_shutdown && !s_session.connected) {
      fprintf(stderr,
              "[producer] Connection lost, retrying in %ums\n",
              reconnect_delay_ms);
      producer_sleep_ms(reconnect_delay_ms);
      reconnect_delay_ms *= 2;
      if (reconnect_delay_ms > RECONNECT_DELAY_MS_MAX) {
        reconnect_delay_ms = RECONNECT_DELAY_MS_MAX;
      }
    }
  }

  // graceful shutdown
  fprintf(stderr,
          "[producer] Shutting down. "
          "frames_received=%llu, frames_dropped=%llu, bytes=%llu\n",
          (unsigned long long)s_session.frames_received,
          (unsigned long long)s_session.frames_dropped,
          (unsigned long long)s_session.bytes_received);

  lws_context_destroy(s_lws_context);
  s_lws_context = NULL;

  free(s_session.assemble_buf);
  s_session.assemble_buf = NULL;

  cb_shutdown(&g_app.buffer); // wake up consumer

  fprintf(stderr, "[producer] Thread exited cleanly\n");
  return NULL;
}
