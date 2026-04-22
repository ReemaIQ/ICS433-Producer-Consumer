/*
 * shared.h
 * --------
 * Central header for the Producer-Consumer Synchronization Service.
 *
 * Defines:
 *  - IPC resource names (shared memory, semaphores)
 *  - The shared memory layout (SharedBuffer)
 *  - Item structure (with priority support)
 *  - Utility macros and constants
 *
 * All processes (controller, producer, consumer) include this header.
 */

#ifndef SHARED_H
#define SHARED_H

#include <stdint.h>
#include <time.h>
#include <sys/types.h>

/* ─── IPC Resource Names ────────────────────────────────────────────────── */

#define SHM_NAME        "/pc_shared_buffer"   /* POSIX shared memory object  */
#define SEM_EMPTY       "/pc_sem_empty"       /* counts empty slots          */
#define SEM_FULL        "/pc_sem_full"        /* counts filled slots         */
#define SEM_MUTEX       "/pc_sem_mutex"       /* mutual exclusion on buffer  */
#define SEM_LOG         "/pc_sem_log"         /* mutual exclusion for logging*/
#define SEM_PROD_DONE   "/pc_sem_prod_done"   /* signals producers finished  */

/* ─── Buffer / Item Limits ──────────────────────────────────────────────── */

#define MAX_BUFFER_SIZE  1024   /* maximum allowed buffer capacity           */
#define MAX_PRODUCERS    64     /* maximum spawnable producer processes      */
#define MAX_CONSUMERS    64     /* maximum spawnable consumer processes      */
#define MAX_ITEMS        1000000/* maximum total items to produce            */

/* ─── Priority Levels (optional extension) ──────────────────────────────── */

#define PRIORITY_NORMAL  0      /* standard item                             */
#define PRIORITY_URGENT  1      /* high-priority item (processed first)      */
#define PRIORITY_LEVELS  2      /* total number of priority levels           */

/* ─── Shutdown / Status Flags ───────────────────────────────────────────── */

#define STATUS_RUNNING   0      /* system is active                          */
#define STATUS_SHUTDOWN  1      /* graceful shutdown requested               */

/* ─── Log File ──────────────────────────────────────────────────────────── */

#define LOG_FILE         "logs/pc_service.log"

/* ─── Item Structure ─────────────────────────────────────────────────────
 *
 * Each item placed in the buffer carries:
 *   seq_num   - globally unique sequence number (1-based)
 *   value     - the "data" produced (for demonstration: equals seq_num)
 *   producer_id - which producer created this item
 *   priority  - PRIORITY_NORMAL or PRIORITY_URGENT
 *   timestamp - wall-clock time of production
 */
typedef struct {
    uint64_t seq_num;           /* unique production sequence number         */
    int      value;             /* payload value                             */
    int      producer_id;       /* PID of the producing process              */
    int      priority;          /* PRIORITY_NORMAL or PRIORITY_URGENT        */
    struct timespec timestamp;  /* time item was produced                    */
} Item;

/* ─── Shared Buffer Structure ────────────────────────────────────────────
 *
 * Stored in POSIX shared memory; accessible by all processes.
 *
 * Circular queue design:
 *   - head  : next slot for a consumer to read from
 *   - tail  : next slot for a producer to write to
 *   - Indices advance with modulo wrap-around (% buffer_size)
 *
 * Priority queues:
 *   - urgent_queue[] holds PRIORITY_URGENT items (consumed before normal)
 *   - buffer[]       holds PRIORITY_NORMAL items
 *   - urgent_count   number of items currently in urgent_queue
 *
 * Counters (updated under mutex) are used for validation:
 *   - total_produced : incremented by producers
 *   - total_consumed : incremented by consumers
 *   - items_requested: the target total set by the controller
 *
 * shutdown_flag: set to STATUS_SHUTDOWN when no more items will be produced;
 *                consumers check this to know when to exit.
 */
typedef struct {
    /* ── normal circular buffer ── */
    Item     buffer[MAX_BUFFER_SIZE];  /* slot array                         */
    int      head;                     /* consumer read index                */
    int      tail;                     /* producer write index               */
    int      buffer_size;              /* capacity (set by controller)       */
    int      count;                    /* current occupancy                  */

    /* ── urgent priority queue (simple bounded array used as FIFO) ── */
    Item     urgent_queue[MAX_BUFFER_SIZE]; /* separate urgent item store    */
    int      urgent_head;              /* urgent consumer read index         */
    int      urgent_tail;             /* urgent producer write index         */
    int      urgent_count;            /* current urgent occupancy            */

    /* ── accounting ── */
    uint64_t total_produced;          /* items inserted across all producers */
    uint64_t total_consumed;          /* items removed  across all consumers */
    uint64_t items_requested;         /* target; set once by controller      */

    /* ── consumed bitmap for duplicate/loss detection ──────────────────
     * One bit per sequence number.  seq_num is 1-based so index = seq-1.
     * Max items = MAX_ITEMS; we store as uint8_t array (1 byte = 8 items).
     */
    uint8_t  consumed_bitmap[(MAX_ITEMS / 8) + 1];

    /* ── control ── */
    int      shutdown_flag;           /* STATUS_RUNNING or STATUS_SHUTDOWN   */
    int      producers_done;          /* how many producers have finished     */
    int      num_producers;           /* total producer count (set by ctrl)  */
} SharedBuffer;

/* ─── Utility Macros ─────────────────────────────────────────────────── */

/* Advance a circular index safely */
#define CIRC_NEXT(idx, size)  (((idx) + 1) % (size))

/* Mark sequence number seq as consumed in the bitmap (seq is 1-based) */
#define BITMAP_SET(bm, seq)   ((bm)[((seq)-1)/8] |=  (1u << (((seq)-1)%8)))

/* Test whether seq has been consumed */
#define BITMAP_TEST(bm, seq)  ((bm)[((seq)-1)/8] &   (1u << (((seq)-1)%8)))

#endif /* SHARED_H */
