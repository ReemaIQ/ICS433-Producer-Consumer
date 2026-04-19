//
// Created by Reema AL-Qahtani on 14/04/2026.
//

/*
* Contains:
* Remove items
* Process/consume
* Logging
 */

// consumer.c
// Uses POSIX shared memory and semaphores for IPC.
//
// Usage (spawned by controller):
//   ./consumer <consumer_id>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <time.h>
#include <errno.h>

// Names must match the controller exactly
#define SHM_NAME        "/prod_cons_shm"
#define SEM_EMPTY_NAME  "/sem_empty"
#define SEM_FULL_NAME   "/sem_full"
#define SEM_MUTEX_NAME  "/sem_mutex"

// One item in the buffer
typedef struct {
    int seq_num;      // unique sequence number assigned by producer
    int producer_id;  // which producer created this item
} Item;

// Shared memory layout — must match controller and producer exactly
typedef struct {
    int buffer_size;           // max slots in the circular buffer
    int head;                  // consumer reads from here
    int tail;                  // producer writes here
    int total_items;           // total items to be produced (target)
    int total_produced;        // items inserted so far
    int total_consumed;        // items removed so far
    int consumed_flags[10000]; // 1 once item with that seq_num is consumed
    Item buffer[1];            // actual size = buffer_size (set by controller)
} SharedBuffer;

// Build a timestamp string like "14:23:01.042"
static void timestamp(char *buf, size_t len)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    struct tm tm_info;
    localtime_r(&ts.tv_sec, &tm_info);

    char base[32];
    strftime(base, sizeof(base), "%H:%M:%S", &tm_info);
    snprintf(buf, len, "%s.%03ld", base, ts.tv_nsec / 1000000);
}

// Print one log line and flush immediately so output is not mixed up
static void log_action(int consumer_id, const char *action, int seq_num, int slot)
{
    char ts[32];
    timestamp(ts, sizeof(ts));

    if (slot >= 0)
        printf("[%s] Consumer %d | %-8s | item seq=%-4d | slot=%d\n",
               ts, consumer_id, action, seq_num, slot);
    else
        printf("[%s] Consumer %d | %-8s | item seq=%-4d\n",
               ts, consumer_id, action, seq_num);

    fflush(stdout);
}

int main(int argc, char *argv[])
{
    // 1. Read consumer ID from command-line argument
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <consumer_id>\n", argv[0]);
        return EXIT_FAILURE;
    }
    int consumer_id = atoi(argv[1]);

    // 2. Open shared memory (already created by the controller)
    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("consumer: shm_open");
        return EXIT_FAILURE;
    }

    // Map just the header first so we can read buffer_size
    size_t header_size = sizeof(SharedBuffer);
    SharedBuffer *shm = mmap(NULL, header_size,
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) {
        perror("consumer: mmap (header)");
        close(shm_fd);
        return EXIT_FAILURE;
    }

    // Now we know buffer_size, remap to the full correct size
    int buf_size = shm->buffer_size;
    size_t full_size = sizeof(SharedBuffer) + (buf_size - 1) * sizeof(Item);
    munmap(shm, header_size);

    shm = mmap(NULL, full_size,
               PROT_READ | PROT_WRITE,
               MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) {
        perror("consumer: mmap (full)");
        close(shm_fd);
        return EXIT_FAILURE;
    }

    // 3. Open the three semaphores (already created by the controller)
    sem_t *sem_empty = sem_open(SEM_EMPTY_NAME, 0);
    sem_t *sem_full  = sem_open(SEM_FULL_NAME,  0);
    sem_t *sem_mutex = sem_open(SEM_MUTEX_NAME, 0);

    if (sem_empty == SEM_FAILED || sem_full == SEM_FAILED || sem_mutex == SEM_FAILED) {
        perror("consumer: sem_open");
        munmap(shm, full_size);
        close(shm_fd);
        return EXIT_FAILURE;
    }

    printf("[Consumer %d] started | PID=%d | buffer_size=%d | total_items=%d\n",
           consumer_id, getpid(), buf_size, shm->total_items);
    fflush(stdout);

    // 4. Main consume loop
    while (1) {

        // Block until at least one filled slot is available
        sem_wait(sem_full);

        // Lock the buffer before reading or modifying anything
        sem_wait(sem_mutex);

        // --- critical section start ---

        // Check if another consumer already finished all the work
        if (shm->total_consumed >= shm->total_items) {
            // Wake up other waiting consumers before leaving
            sem_post(sem_mutex);
            sem_post(sem_full);
            break;
        }

        // Read item from the head slot
        int slot = shm->head;
        Item item = shm->buffer[slot];

        // Move head forward (wrap around at end of buffer)
        shm->head = (shm->head + 1) % buf_size;

        // Update counters and mark item as consumed for validation
        shm->total_consumed++;
        if (item.seq_num >= 0 && item.seq_num < 10000)
            shm->consumed_flags[item.seq_num] = 1;

        int consumed_so_far = shm->total_consumed;
        int all_done        = (consumed_so_far >= shm->total_items);

        // --- critical section end ---
        sem_post(sem_mutex);

        // Signal that one more empty slot is now free for producers
        sem_post(sem_empty);

        // Log what we just consumed (done outside the critical section)
        log_action(consumer_id, "REMOVED",  item.seq_num, slot);
        log_action(consumer_id, "CONSUMED", item.seq_num, -1);
        printf("          └─ produced by Producer %d | consumed=%d/%d\n",
               item.producer_id, consumed_so_far, shm->total_items);
        fflush(stdout);

        // Stop when all items have been consumed
        if (all_done)
            break;
    }

    // 5. Print final summary
    printf("[Consumer %d] finished | total_consumed so far = %d\n",
           consumer_id, shm->total_consumed);
    fflush(stdout);

    // 6. Release resources — do NOT unlink, that is the controller's job
    munmap(shm, full_size);
    close(shm_fd);
    sem_close(sem_empty);
    sem_close(sem_full);
    sem_close(sem_mutex);

    return EXIT_SUCCESS;
}
