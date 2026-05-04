 /*
 * producer.c
 * ----------
 * Producer process for the Producer-Consumer Synchronization Service.
 *
 * Each producer is launched as a separate process by the controller.
 * It receives its identity and workload via command-line arguments.
 *
 * Usage (internal – called by controller):
 *   ./producer <producer_id> <num_items> <urgent_pct>
 *
 *   producer_id : integer ID (0-based) to identify this producer in logs
 *   num_items   : number of items this producer must insert
 *   urgent_pct  : percentage (0-100) of items to mark PRIORITY_URGENT
 *
 * Algorithm (per item):
 *   1. sem_wait(empty)   — wait for at least one free slot.
 *   2. sem_wait(mutex)   — enter critical section.
 *   3. Claim the next unique sequence number (shared->total_produced + 1).
 *   4. Build the Item and insert it:
 *        - PRIORITY_URGENT  → urgent_queue  (separate FIFO)
 *        - PRIORITY_NORMAL  → circular buffer
 *   5. Increment total_produced.
 *   6. sem_post(mutex)   — exit critical section.
 *   7. sem_post(full)    — signal one item is available to consumers.
 *
 * Shutdown:
 *   Before each production cycle the producer checks shutdown_flag.
 *   If STATUS_SHUTDOWN is set it stops early (signal-triggered exit).
 *   When a producer finishes all its items it increments producers_done
 *   under the mutex; the last producer also sets shutdown_flag so that
 *   consumers know no more items are coming.
 */
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
 
#include "shared.h"
#include "logger.h"
 
/* ─── Helper: simulate production work ──────────────────────────────── */
static void simulate_produce(void)
{
    /* Sleep 0–7 ms to mimic real work and create interleaving */
    struct timespec ts = { 0, (rand() % 8) * 1000000L };
    nanosleep(&ts, NULL);
}
 
/* ─── Main ───────────────────────────────────────────────────────────── */
 
int main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <producer_id> <num_items> <urgent_pct>\n",
                argv[0]);
        return EXIT_FAILURE;
    }
 
    int      producer_id = atoi(argv[1]);
    uint64_t num_items   = (uint64_t)strtoull(argv[2], NULL, 10);
    int      urgent_pct  = atoi(argv[3]);
 
    /* Clamp urgent_pct to a valid range */
    if (urgent_pct < 0)   urgent_pct = 0;
    if (urgent_pct > 100) urgent_pct = 100;
 
    srand((unsigned)(time(NULL) ^ getpid()));
 
    /* ── Attach to shared memory ────────────────────────────────────────── */
    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (shm_fd < 0) {
        perror("producer: shm_open");
        return EXIT_FAILURE;
    }
 
    SharedBuffer *shm = (SharedBuffer *)mmap(NULL, sizeof(SharedBuffer),
                                              PROT_READ | PROT_WRITE,
                                              MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) {
        perror("producer: mmap");
        close(shm_fd);
        return EXIT_FAILURE;
    }
    close(shm_fd);   /* fd no longer needed after mmap */
 
    /* ── Open semaphores ────────────────────────────────────────────────── */
    sem_t *sem_empty = sem_open(SEM_EMPTY, 0);
    sem_t *sem_full  = sem_open(SEM_FULL,  0);
    sem_t *sem_mutex = sem_open(SEM_MUTEX, 0);
 
    if (sem_empty == SEM_FAILED || sem_full == SEM_FAILED ||
        sem_mutex == SEM_FAILED) {
        perror("producer: sem_open");
        munmap(shm, sizeof(SharedBuffer));
        return EXIT_FAILURE;
    }
 
    /* ── Initialise logging ─────────────────────────────────────────────── */
    if (log_init() != 0) {
        fprintf(stderr, "producer %d: log_init failed\n", producer_id);
    }
 
    LOG_I("Producer %d started (pid=%d, items=%llu, urgent_pct=%d%%)",
          producer_id, (int)getpid(),
          (unsigned long long)num_items, urgent_pct);
 
    /* ── Production loop ────────────────────────────────────────────────── */
    for (uint64_t i = 0; i < num_items; i++) {
 
        /* Check for early shutdown (e.g., SIGINT received by controller) */
        if (shm->shutdown_flag == STATUS_SHUTDOWN) {
            LOG_W("Producer %d: shutdown flag detected – stopping early at item %llu",
                  producer_id, (unsigned long long)i);
            break;
        }
 
        /* Simulate the time it takes to "produce" the item */
        simulate_produce();
 
        /* ── Step 1: Wait for a free slot ───────────────────────────────── */
        if (sem_wait(sem_empty) != 0) {
            if (errno == EINTR) { i--; continue; }   /* retry on signal     */
            perror("producer: sem_wait(empty)");
            break;
        }
 
        /* ── Step 2: Enter critical section ─────────────────────────────── */
        if (sem_wait(sem_mutex) != 0) {
            if (errno == EINTR) { sem_post(sem_empty); i--; continue; }
            perror("producer: sem_wait(mutex)");
            sem_post(sem_empty);
            break;
        }
 
        /* ── Step 3: Claim a unique sequence number ──────────────────────── */
        uint64_t seq = shm->total_produced + 1;
 
        /* ── Step 4: Build the item ──────────────────────────────────────── */
        Item item;
        item.seq_num     = seq;
        item.value       = (int)seq;           /* payload == sequence number  */
        item.producer_id = producer_id;
        item.priority    = ((rand() % 100) < urgent_pct)
                               ? PRIORITY_URGENT
                               : PRIORITY_NORMAL;
        clock_gettime(CLOCK_REALTIME, &item.timestamp);
 
        /* ── Step 5: Insert into the correct queue ───────────────────────── */
        int  slot;
        const char *queue_name;
 
        if (item.priority == PRIORITY_URGENT) {
            /*
             * Urgent queue is a separate bounded FIFO.
             * The sem_empty semaphore counts total free capacity across both
             * queues (since every item occupies one logical "slot").
             */
            slot = shm->urgent_tail;
            shm->urgent_queue[shm->urgent_tail] = item;
            shm->urgent_tail = CIRC_NEXT(shm->urgent_tail, shm->buffer_size);
            shm->urgent_count++;
            queue_name = "URGENT";
        } else {
            slot = shm->tail;
            shm->buffer[shm->tail] = item;
            shm->tail  = CIRC_NEXT(shm->tail, shm->buffer_size);
            shm->count++;
            queue_name = "NORMAL";
        }
 
        /* ── Step 6: Update accounting ───────────────────────────────────── */
        shm->total_produced++;
 
        /* ── Step 7: Exit critical section ──────────────────────────────── */
        sem_post(sem_mutex);
 
        /* ── Step 8: Signal one item is available ────────────────────────── */
        sem_post(sem_full);
 
        LOG_I("Producer %d | seq=%-6llu | val=%-5d | priority=%s | slot=%d | produced",
              producer_id,
              (unsigned long long)seq,
              item.value,
              queue_name,
              slot);
    }
 
    /* ── Mark this producer as done; last one triggers shutdown ─────────── */
    if (sem_wait(sem_mutex) == 0) {
        shm->producers_done++;
        if (shm->producers_done == shm->num_producers) {
            shm->shutdown_flag = STATUS_SHUTDOWN;
            LOG_I("Producer %d: last producer – setting shutdown flag", producer_id);
        }
        sem_post(sem_mutex);
    }
 
    LOG_I("Producer %d finished (pid=%d)", producer_id, (int)getpid());
 
    /* ── Cleanup ─────────────────────────────────────────────────────────── */
    sem_close(sem_empty);
    sem_close(sem_full);
    sem_close(sem_mutex);
    munmap(shm, sizeof(SharedBuffer));
    log_close();
 
    return EXIT_SUCCESS;
}