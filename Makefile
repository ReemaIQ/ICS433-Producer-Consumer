# ─────────────────────────────────────────────────────────────────────────────
# Makefile — Producer-Consumer Synchronization Service
#
# Targets:
#   all        build all binaries (default)
#   controller build only the controller
#   producer   build only the producer
#   consumer   build only the consumer
#   clean      remove compiled objects and binaries
#   run        quick smoke-test: 10-slot buffer, 3 prod, 2 cons, 50 items
#   runbig     stress test: 64-slot buffer, 8 prod, 4 cons, 10000 items
#   runurgent  test with 40% urgent items
#   valgrind   run smoke-test under valgrind (if installed)
# ─────────────────────────────────────────────────────────────────────────────

CC      := gcc
CFLAGS  := -Wall -Wextra -Wpedantic -O2 -g \
           -I./include \
           -std=c11 \
           -D_GNU_SOURCE   # needed for clock_gettime, sem_timedwait etc.
LDFLAGS := -lrt -lpthread  # link POSIX realtime and pthread libraries

# Source directories
SRC     := src
INC     := include

# Object files (shared logger compiled into each binary via per-target rule)
LOGGER_SRC := $(SRC)/logger.c
LOGGER_OBJ := logger.o

CTRL_SRCS   := $(SRC)/controller.c
PROD_SRCS   := $(SRC)/producer.c
CONS_SRCS   := $(SRC)/consumer.c

.PHONY: all clean run runbig runurgent valgrind

# ─── Default target ──────────────────────────────────────────────────────────
all: controller producer consumer

# ─── Build rules ─────────────────────────────────────────────────────────────

# Compile logger object (shared by all binaries)
$(LOGGER_OBJ): $(LOGGER_SRC) $(INC)/logger.h $(INC)/shared.h
	$(CC) $(CFLAGS) -c -o $@ $<

controller: $(CTRL_SRCS) $(LOGGER_OBJ) $(INC)/shared.h $(INC)/logger.h
	$(CC) $(CFLAGS) -o $@ $(CTRL_SRCS) $(LOGGER_OBJ) $(LDFLAGS)

producer: $(PROD_SRCS) $(LOGGER_OBJ) $(INC)/shared.h $(INC)/logger.h
	$(CC) $(CFLAGS) -o $@ $(PROD_SRCS) $(LOGGER_OBJ) $(LDFLAGS)

consumer: $(CONS_SRCS) $(LOGGER_OBJ) $(INC)/shared.h $(INC)/logger.h
	$(CC) $(CFLAGS) -o $@ $(CONS_SRCS) $(LOGGER_OBJ) $(LDFLAGS)

# ─── Clean ───────────────────────────────────────────────────────────────────
clean:
	rm -f controller producer consumer $(LOGGER_OBJ)
	rm -rf logs/pc_service.log

# ─── Quick smoke test ────────────────────────────────────────────────────────
run: all
	@mkdir -p logs
	./controller 10 3 2 50

# ─── Stress test ─────────────────────────────────────────────────────────────
runbig: all
	@mkdir -p logs
	./controller 64 8 4 10000

# ─── Urgent priority test ────────────────────────────────────────────────────
runurgent: all
	@mkdir -p logs
	./controller 10 3 2 50 --urgent 40

# ─── Valgrind memory/resource check ──────────────────────────────────────────
valgrind: all
	@mkdir -p logs
	valgrind --leak-check=full --track-origins=yes \
	         --trace-children=yes --error-exitcode=1 \
	         ./controller 8 2 2 20
