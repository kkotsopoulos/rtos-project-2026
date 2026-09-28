# Makefile — RTOS Final Project 2026 

CC := gcc

CFLAGS := -Wall -Wextra -Wpedantic -Wformat=2 -Wshadow \
          -Wno-unused-parameter \
          -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L \
          -std=c11 \
          -I./include

RELEASE_FLAGS := -O2 -fstack-protector-strong -pipe
DEBUG_FLAGS := -O0 -g3 -DDEBUG -fsanitize=address,undefined
TEST_FLAGS := -O0 -g3 -DDEBUG -fsanitize=thread

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    LIBS      := -lwebsockets -lcjson -lpthread -lrt
else
    LIBS      := -lwebsockets -lcjson -lpthread
endif

TEST_LIBS := $(filter-out -lwebsockets -lcjson, $(LIBS))

SRC_DIR   := src
INC_DIR   := include
BUILD_DIR := build

APP_SRCS  := $(SRC_DIR)/circular_buffer.c \
             $(SRC_DIR)/globals.c         \
             $(SRC_DIR)/producer.c        \
             $(SRC_DIR)/consumer.c        \
             $(SRC_DIR)/monitor.c         \
             $(SRC_DIR)/main.c

APP_OBJS  := $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(APP_SRCS))
APP_BIN   := $(BUILD_DIR)/rtos_firehose

TEST_SRCS := $(SRC_DIR)/test_circular_buffer.c \
             $(SRC_DIR)/circular_buffer.c
TEST_BIN  := $(BUILD_DIR)/test_cb

PI_HOST    ?= pi@raspberrypi.local
PI_REMOTE  := ~/rtos
PI_SESSION := firehose_24h
PI_BIN     := $(PI_REMOTE)/$(APP_BIN)
LOCAL_LOG  := ./metrics_log.txt

# local targets 

.PHONY: all
all: release

.PHONY: release
release: CFLAGS += $(RELEASE_FLAGS)
release: $(BUILD_DIR) $(APP_BIN)
	@echo ""
	@echo "==> Build successful: $(APP_BIN)"

$(APP_BIN): $(APP_OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

.PHONY: debug
debug: CFLAGS += $(DEBUG_FLAGS)
debug: $(BUILD_DIR) $(APP_OBJS)
	$(CC) $(CFLAGS) $(APP_OBJS) -o $(BUILD_DIR)/rtos_firehose_debug $(LIBS)
	@echo "==> Debug build: $(BUILD_DIR)/rtos_firehose_debug"

.PHONY: test  # unit tests with TSan
test: CFLAGS += $(TEST_FLAGS)
test: $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_SRCS) -o $(TEST_BIN) $(TEST_LIBS)
	@echo ""
	@echo "==> Running circular buffer unit tests..."
	@./$(TEST_BIN)

.PHONY: check  # cppcheck static analysis
check:
	@which cppcheck > /dev/null 2>&1 || \
		(echo "cppcheck not found. Install: brew install cppcheck" && exit 1)
	cppcheck --enable=all --std=c11 \
	         --suppress=missingIncludeSystem \
	         -I$(INC_DIR) \
	         $(APP_SRCS)
	@echo "==> cppcheck complete."

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)
	@echo "==> Build directory cleaned."

# pipeline targets 

.PHONY: push  # rsync + build on Pi
push: deploy build-pi
	@echo ""
	@echo "==> Push & build complete. Run: make run"

.PHONY: push-run  # rsync + build + start 24h session
push-run: deploy build-pi run
	@echo ""
	@echo "==> Pipeline complete. Logging started on Pi."
	@echo "    make tail-log | make fetch-log | make stop"

.PHONY: install-deps  # install lws + cjson on Pi via SSH
install-deps:
	@echo "==> Installing dependencies on $(PI_HOST) ..."
	ssh $(PI_HOST) "sudo apt-get update -qq && \
	                sudo apt-get install -y libwebsockets-dev libcjson-dev screen"
	@echo "==> Dependencies installed."

.PHONY: deploy  # rsync changed sources to Pi
deploy:
	@echo "==> Syncing sources to $(PI_HOST):$(PI_REMOTE) ..."
	ssh $(PI_HOST) "mkdir -p $(PI_REMOTE)/src $(PI_REMOTE)/include $(PI_REMOTE)/build"
	rsync -avz --progress \
	      --exclude='build/'      \
	      --exclude='.venv/'      \
	      --exclude='stubs/'      \
	      --exclude='.git/'       \
	      --exclude='*.DS_Store'  \
	      --exclude='*.log'       \
	      --exclude='*.o'         \
	      $(SRC_DIR)/   $(PI_HOST):$(PI_REMOTE)/src/
	rsync -avz --progress \
	      $(INC_DIR)/   $(PI_HOST):$(PI_REMOTE)/include/
	rsync -avz --progress \
	      Makefile       $(PI_HOST):$(PI_REMOTE)/Makefile
	@echo "==> Sync complete."

.PHONY: build-pi  # compile on Pi via SSH
build-pi:
	@echo "==> Building on $(PI_HOST) ..."
	ssh $(PI_HOST) "cd $(PI_REMOTE) && make release"
	@echo "==> Build on Pi complete: $(PI_BIN)"

#  session management 

.PHONY: run  # start detached screen session on Pi (survives SSH disconnect)
run:
	@echo "==> Starting 24h screen session '$(PI_SESSION)' on $(PI_HOST) ..."
	-ssh $(PI_HOST) "screen -S $(PI_SESSION) -X quit 2>/dev/null; true"
	@sleep 1
	ssh $(PI_HOST) " \
	    cd $(PI_REMOTE) && \
	    screen -dmS $(PI_SESSION) -L -Logfile screen_$(PI_SESSION).log \
	        bash -c './$(APP_BIN) 2>stderr_$(PI_SESSION).log; \
	                 echo \"[session] Process exited at \$$(date)\" \
	                 >> stderr_$(PI_SESSION).log' \
	"
	@sleep 2
	@echo ""
	@echo "==> Session '$(PI_SESSION)' started."
	@echo "    make attach | make status | make tail-log | make stop"

.PHONY: run-fg  # foreground run for debugging 
run-fg:
	@echo "==> Running in foreground on $(PI_HOST) (Ctrl+C to stop) ..."
	ssh -t $(PI_HOST) "cd $(PI_REMOTE) && ./$(APP_BIN)"

.PHONY: status  # check if process is alive + screen session info
status:
	@echo "==> Status on $(PI_HOST) ..."
	@echo "Screen sessions"
	ssh $(PI_HOST) "screen -ls 2>&1 || true"
	@echo "Process status"
	ssh $(PI_HOST) "pgrep -a rtos_firehose 2>/dev/null || echo '  (not running)'"
	@echo "Log tail (last 5 lines)"
	ssh $(PI_HOST) "tail -5 $(PI_REMOTE)/metrics_log.txt 2>/dev/null \
	               || echo '  (log not found yet)'"

.PHONY: attach  # connect to screen session 
attach:
	@echo "==> Attaching to session '$(PI_SESSION)' (detach: Ctrl+A D) ..."
	ssh -t $(PI_HOST) "screen -r $(PI_SESSION)"

.PHONY: stop  # send SIGINT for graceful shutdown
stop:
	@echo "==> Sending SIGINT to rtos_firehose on $(PI_HOST) ..."
	ssh $(PI_HOST) "pkill -SIGINT -x rtos_firehose 2>/dev/null && \
	               echo '  Graceful shutdown initiated.' || \
	               echo '  Process not found (already stopped?).'"
	@sleep 3
	@echo "==> Verifying process stopped ..."
	ssh $(PI_HOST) "pgrep rtos_firehose 2>/dev/null && \
	               echo '  WARNING: still running!' || \
	               echo '  Process stopped cleanly.'"

# log management 

.PHONY: tail-log  # live tail of metrics_log.txt from Pi
tail-log:
	@echo "==> Live tail of metrics_log.txt from $(PI_HOST) (Ctrl+C to quit) ..."
	@echo "==> Format: Seconds,Nanoseconds,Commit,Identity,Account,Info,Buffer%,CPU%"
	ssh $(PI_HOST) "tail -f $(PI_REMOTE)/metrics_log.txt"

DEST ?= $(LOCAL_LOG)

.PHONY: fetch-log  # download metrics_log.txt from Pi
fetch-log:
	@echo "==> Fetching metrics_log.txt from $(PI_HOST) ..."
	rsync -avz --progress \
	      $(PI_HOST):$(PI_REMOTE)/metrics_log.txt \
	      $(DEST)
	@echo ""
	@echo "==> Log saved to: $(DEST)"
	@echo "    Lines: $$(wc -l < $(DEST)) (including header)"
	@echo "    Size : $$(du -sh $(DEST) | cut -f1)"

.PHONY: fetch-all  # download metrics_log.txt + stderr log
fetch-all: fetch-log
	@echo "==> Fetching stderr log ..."
	rsync -avz --progress \
	      $(PI_HOST):$(PI_REMOTE)/stderr_$(PI_SESSION).log \
	      ./stderr_$(PI_SESSION).log 2>/dev/null || \
	      echo "  (no stderr log found)"
	@echo "==> All logs fetched."

.PHONY: help
help:
	@echo ""
	@echo "  RTOS Final Project 2026 — Makefile targets"
	@echo ""
	@echo "  Local:"
	@echo "    make test              unit tests (TSan)"
	@echo "    make check             cppcheck"
	@echo "    make clean             remove build/"
	@echo ""
	@echo "  Pipeline:"
	@echo "    make push              rsync + build on Pi"
	@echo "    make push-run          rsync + build + start 24h session"
	@echo ""
	@echo "  Step by step:"
	@echo "    make install-deps      install lws+cjson on Pi"
	@echo "    make deploy            rsync sources to Pi"
	@echo "    make build-pi          compile on Pi (SSH)"
	@echo "    make run               start detached screen session"
	@echo "    make run-fg            foreground run (for debug)"
	@echo ""
	@echo "  Session:"
	@echo "    make status            check process / screen"
	@echo "    make attach            attach to screen (Ctrl+A D to detach)"
	@echo "    make stop              graceful shutdown (SIGINT)"
	@echo ""
	@echo "  Logs:"
	@echo "    make tail-log          live tail from Pi"
	@echo "    make fetch-log         download metrics_log.txt"
	@echo "    make fetch-all         download all logs"
	@echo ""
	@echo "  Variables:"
	@echo "    PI_HOST=pi@<ip>        (default: pi@raspberrypi.local)"
	@echo "    DEST=<path>            (default: ./metrics_log.txt)"
	@echo ""
	@echo "  Example:  make push-run PI_HOST=pi@192.168.1.42"
	@echo ""
