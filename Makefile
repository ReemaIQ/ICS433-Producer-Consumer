#Contains:
#Compilation rules
#Build targets (controller, producer, consumer)
#Clean command

CC      = gcc
CFLAGS  = -Wall -Wextra -g -Iinclude
LDFLAGS = -lrt -lpthread

SRCDIR = src
BINDIR = .

SRCS_COMMON = $(SRCDIR)/logger.c
SRCS_CTRL   = $(SRCDIR)/controller.c $(SRCS_COMMON)
SRCS_PROD   = $(SRCDIR)/producer.c   $(SRCS_COMMON)
SRCS_CONS   = $(SRCDIR)/consumer.c   $(SRCS_COMMON)

all: controller producer consumer

controller: $(SRCS_CTRL)
	$(CC) $(CFLAGS) -o $(BINDIR)/controller $(SRCS_CTRL) $(LDFLAGS)

producer: $(SRCS_PROD)
	$(CC) $(CFLAGS) -o $(BINDIR)/producer $(SRCS_PROD) $(LDFLAGS)

consumer: $(SRCS_CONS)
	$(CC) $(CFLAGS) -o $(BINDIR)/consumer $(SRCS_CONS) $(LDFLAGS)

clean:
	rm -f controller producer consumer logs/*.log

# Remove any leftover IPC objects (useful during development)
ipc-clean:
	-rm -f /dev/shm/pc_buffer
	-rm -f /dev/shm/sem.pc_sem_empty /dev/shm/sem.pc_sem_full /dev/shm/sem.pc_sem_mutex

.PHONY: all clean ipc-clean
