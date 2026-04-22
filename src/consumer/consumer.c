//
// Created by Reema AL-Qahtani on 14/04/2026.
// Edited by Sara Esmaeil on 22/04/2026.
/*
 * consumer.c
 * ----------
 * Consumer process for the Producer-Consumer Synchronization Service.
 *
 * Each consumer is launched as a separate process by the controller.
 * It receives its identity via command-line arguments.
 *
 * Usage (internal – called by controller):
 *   ./consumer <consumer_id>
 *
 *   consumer_id : integer ID (0-based) to identify this consumer in logs
 *
 * Algorithm (per item):
 *   1. sem_wait(full)    — wait for at least one available item.
 *   2. sem_wait(mutex)   — enter critical section.
 *   3. Prefer urgent_queue over normal buffer (priority extension).
 *   4. Remove item; update head/count and total_consumed.
 *   5. Mark item's seq_num in consumed_bitmap (duplicate detection).
 *   6. sem_post(mutex)   — exit critical section.
 *   7. sem_post(empty)   — signal a slot has been freed.
 *   8. Process/log the item.
 *
 * Exit condition:
 *   After sem_wait(full) returns, the consumer re-checks whether the
 *   buffer is truly empty AND the shutdown flag is set.  If so, it
 *   posts sem_full back (to allow sibling consumers to also drain) and
 *   exits cleanly.
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

/* ─── Helper: simulate consumption work ─────────────────────────────── */
static void simulate_consume(void)
{
    struct timespec ts = { 0, (rand() % 8) * 1000000L };
    nanosleep(&ts, NULL);
}

/* ─── Main ───────────────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <consumer_id>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int consumer_id = atoi(argv[1]);

    srand((unsigned)(time(NULL) ^ getpid()));

    /* ── Attach to shared memory ────────────────────────────────────────── */
    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (shm_fd < 0) {
        perror("consumer: shm_open");
        return EXIT_FAILURE;
    }

    SharedBuffer *shm = (SharedBuffer *)mmap(NULL, sizeof(SharedBuffer),
                                              PROT_READ | PROT_WRITE,
                                              MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) {
        perror("consumer: mmap");
        close(shm_fd);
        return EXIT_FAILURE;
    }
    close(shm_fd);

    /* ── Open semaphores ────────────────────────────────────────────────── */
    sem_t *sem_empty = sem_open(SEM_EMPTY, 0);
    sem_t *sem_full  = sem_open(SEM_FULL,  0);
    sem_t *sem_mutex = sem_open(SEM_MUTEX, 0);

    if (sem_empty == SEM_FAILED || sem_full == SEM_FAILED ||
        sem_mutex == SEM_FAILED) {
        perror("consumer: sem_open");
        munmap(shm, sizeof(SharedBuffer));
        return EXIT_FAILURE;
    }

    /* ── Initialise logging ─────────────────────────────────────────────── */
    if (log_init() != 0) {
        fprintf(stderr, "consumer %d: log_init failed\n", consumer_id);
    }

    LOG_I("Consumer %d started (pid=%d)", consumer_id, (int)getpid());

    /* ── Consumption loop ───────────────────────────────────────────────── */
    while (1) {

        /* ── Step 1: Wait for an available item ─────────────────────────── */
        if (sem_wait(sem_full) != 0) {
            if (errno == EINTR) continue;   /* interrupted – retry           */
            perror("consumer: sem_wait(full)");
            break;
        }

        /* ── Step 2: Enter critical section ─────────────────────────────── */
        if (sem_wait(sem_mutex) != 0) {
            if (errno == EINTR) { sem_post(sem_full); continue; }
            perror("consumer: sem_wait(mutex)");
            sem_post(sem_full);
            break;
        }

        /*
         * ── Step 3: Check exit condition ──────────────────────────────────
         *
         * The semaphore said there was an item, but by the time we hold
         * the mutex another consumer may have taken it.  Also, if both
         * counts are zero AND shutdown is set, we are done.
         */
        if (shm->count == 0 && shm->urgent_count == 0) {
            if (shm->shutdown_flag == STATUS_SHUTDOWN) {
                /* All done – release mutex, post token back for each sibling */
                sem_post(sem_mutex);
                sem_post(sem_full);   /* one extra post to wake next consumer */
                LOG_I("Consumer %d: buffer empty + shutdown – exiting loop",
                      consumer_id);
                break;
            }
            /*
             * Spurious wake (shouldn't happen with correct semaphore use,
             * but guard defensively): release and re-wait.
             */
            sem_post(sem_mutex);
            continue;
        }

        /* ── Step 4: Remove item (urgent takes priority) ─────────────────── */
        Item item;
        int  slot;
        const char *queue_name;

        if (shm->urgent_count > 0) {
            /* Drain urgent queue first */
            slot       = shm->urgent_head;
            item       = shm->urgent_queue[shm->urgent_head];
            shm->urgent_head  = CIRC_NEXT(shm->urgent_head, shm->buffer_size);
            shm->urgent_count--;
            queue_name = "URGENT";
        } else {
            /* Take from normal circular buffer */
            slot       = shm->head;
            item       = shm->buffer[shm->head];
            shm->head  = CIRC_NEXT(shm->head, shm->buffer_size);
            shm->count--;
            queue_name = "NORMAL";
        }

        /* ── Step 5: Record consumption ──────────────────────────────────── */
        shm->total_consumed++;

        /*
         * Mark this sequence number in the bitmap.
         * If already marked → duplicate consumption detected!
         */
        if (BITMAP_TEST(shm->consumed_bitmap, item.seq_num)) {
            LOG_W("Consumer %d: DUPLICATE consumption of seq=%llu (priority=%s)",
                  consumer_id, (unsigned long long)item.seq_num, queue_name);
        } else {
            BITMAP_SET(shm->consumed_bitmap, item.seq_num);
        }

        /* ── Step 6: Exit critical section ──────────────────────────────── */
        sem_post(sem_mutex);

        /* ── Step 7: Signal a slot is now free ───────────────────────────── */
        sem_post(sem_empty);

        /* ── Step 8: Process item ────────────────────────────────────────── */

        /* Calculate latency from production to consumption */
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        long latency_us = (long)((now.tv_sec  - item.timestamp.tv_sec)  * 1000000L
                               + (now.tv_nsec - item.timestamp.tv_nsec) / 1000L);

        LOG_I("Consumer %d | seq=%-6llu | val=%-5d | priority=%s | slot=%d | "
              "producer=%d | latency=%ldus | consumed",
              consumer_id,
              (unsigned long long)item.seq_num,
              item.value,
              queue_name,
              slot,
              item.producer_id,
              latency_us);

        /* Simulate variable consumption time */
        simulate_consume();
    }

    LOG_I("Consumer %d finished (pid=%d, consumed≈%llu)",
          consumer_id, (int)getpid(),
          (unsigned long long)shm->total_consumed);   /* approximate */

    /* ── Cleanup ─────────────────────────────────────────────────────────── */
    sem_close(sem_empty);
    sem_close(sem_full);
    sem_close(sem_mutex);
    munmap(shm, sizeof(SharedBuffer));
    log_close();

    return EXIT_SUCCESS;
}
