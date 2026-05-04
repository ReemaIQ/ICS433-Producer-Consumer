/*
 * controller.c
 * ------------
 * Controller process for the Producer-Consumer Synchronization Service.
 *
 * The controller is the single entry point for the entire system.  It:
 *   1. Parses and validates command-line arguments.
 *   2. Creates (or re-creates) the shared memory region.
 *   3. Creates (or re-creates) all named semaphores.
 *   4. Initializes the SharedBuffer structure.
 *   5. Spawns N producer processes and M consumer processes.
 *   6. Waits for all child processes to terminate (waitpid loop).
 *   7. Runs a validation pass over the shared memory.
 *   8. Prints a summary report.
 *   9. Cleans up all IPC resources (munmap, shm_unlink, sem_unlink).
 *
 * Usage:
 *   ./controller <buffer_size> <num_producers> <num_consumers> <total_items>
 *                [--urgent <pct>] [--validate] [--help]
 *
 * Options:
 *   --urgent  <pct>  Percentage (0-100) of items marked PRIORITY_URGENT.
 *                    Default: 20.
 *   --validate       Run full validation after completion (default: on).
 *   --help           Print usage and exit.
 *
 * Example:
 *   ./controller 10 3 2 100
 *   ./controller 10 3 2 100 --urgent 30
 *
 * Item distribution among producers:
 *   Items are distributed as evenly as possible.  If total_items is not
 *   divisible by num_producers, the first (total_items % num_producers)
 *   producers each receive one extra item.
 *
 * Signals:
 *   SIGINT / SIGTERM trigger graceful shutdown: the controller sets the
 *   shutdown flag in shared memory, then waits for children to finish.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <semaphore.h>

#include "shared.h"
#include "logger.h"

/* ─── Global pointer to shared memory (needed by signal handler) ─────── */
static SharedBuffer *g_shm = NULL;

/* ─── Signal handler for graceful shutdown ───────────────────────────── */
static void handle_signal(int signo)
{
    (void)signo;
    if (g_shm) {
        g_shm->shutdown_flag = STATUS_SHUTDOWN;
    }
    /* Write directly; log subsystem may not be async-signal-safe */
    const char msg[] = "\n[CTRL] Signal received - requesting shutdown\n";
    ssize_t _r = write(STDOUT_FILENO, msg, sizeof(msg)-1); (void)_r;
}

/* ─── cleanup_ipc: remove all IPC objects ────────────────────────────── */
static void cleanup_ipc(SharedBuffer *shm)
{
    if (shm && shm != MAP_FAILED) {
        munmap(shm, sizeof(SharedBuffer));
    }
    shm_unlink(SHM_NAME);

    sem_unlink(SEM_EMPTY);
    sem_unlink(SEM_FULL);
    sem_unlink(SEM_MUTEX);
    sem_unlink(SEM_LOG);
    sem_unlink(SEM_PROD_DONE);
}

/* ─── usage ──────────────────────────────────────────────────────────── */
static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s <buffer_size> <num_producers> <num_consumers> <total_items>\n"
        "           [--urgent <pct>] [--no-validate] [--help]\n\n"
        "  buffer_size    : capacity of the shared circular buffer (1-%d)\n"
        "  num_producers  : number of producer processes      (1-%d)\n"
        "  num_consumers  : number of consumer processes      (1-%d)\n"
        "  total_items    : total items to produce            (1-%d)\n\n"
        "Options:\n"
        "  --urgent <pct> : %% of items as PRIORITY_URGENT    (0-100, default 20)\n"
        "  --no-validate  : skip post-run validation pass\n"
        "  --help         : show this message\n\n"
        "Example:\n"
        "  %s 10 3 2 100\n"
        "  %s 10 3 2 100 --urgent 30\n",
        prog,
        MAX_BUFFER_SIZE, MAX_PRODUCERS, MAX_CONSUMERS, MAX_ITEMS,
        prog, prog);
}

/* ─── validate: post-run correctness check ───────────────────────────── */
static int validate(SharedBuffer *shm, uint64_t items_requested)
{
    int ok = 1;

    printf("\n╔══════════════════════════════════════════════╗\n");
    printf("║            VALIDATION REPORT                 ║\n");
    printf("╚══════════════════════════════════════════════╝\n");

    /* ── (1) Total produced vs requested ─── */
    printf("Items requested  : %llu\n",  (unsigned long long)items_requested);
    printf("Items produced   : %llu\n",  (unsigned long long)shm->total_produced);
    printf("Items consumed   : %llu\n\n", (unsigned long long)shm->total_consumed);

    if (shm->total_produced != items_requested) {
        printf("[FAIL] produced (%llu) != requested (%llu)\n",
               (unsigned long long)shm->total_produced,
               (unsigned long long)items_requested);
        ok = 0;
    } else {
        printf("[PASS] produced == requested (%llu)\n",
               (unsigned long long)items_requested);
    }

    if (shm->total_consumed != shm->total_produced) {
        printf("[FAIL] consumed (%llu) != produced (%llu)\n",
               (unsigned long long)shm->total_consumed,
               (unsigned long long)shm->total_produced);
        ok = 0;
    } else {
        printf("[PASS] consumed == produced (%llu)\n",
               (unsigned long long)shm->total_produced);
    }

    /* ── (2) Bitmap scan: check every seq number 1..total_produced ─── */
    uint64_t missing   = 0;

    /*
     * We set bits as items were consumed, so any sequence number in
     * [1, total_produced] that is NOT set is a lost item.
     */
    for (uint64_t seq = 1; seq <= shm->total_produced; seq++) {
        if (!BITMAP_TEST(shm->consumed_bitmap, seq)) {
            missing++;
            if (missing <= 10) {   /* print first 10 missing items */
                printf("[FAIL] seq=%llu was produced but never consumed (LOST)\n",
                       (unsigned long long)seq);
            }
        }
    }

    if (missing > 10) {
        printf("       ... and %llu more missing items\n",
               (unsigned long long)(missing - 10));
    }

    if (missing == 0) {
        printf("[PASS] No lost items detected\n");
    } else {
        printf("[FAIL] %llu item(s) lost\n", (unsigned long long)missing);
        ok = 0;
    }

    /* ── (3) Buffer should be empty at end ─── */
    if (shm->count != 0 || shm->urgent_count != 0) {
        printf("[FAIL] Buffer not empty at end: count=%d urgent=%d\n",
               shm->count, shm->urgent_count);
        ok = 0;
    } else {
        printf("[PASS] Buffer is empty at end\n");
    }

    printf("\n%s\n\n",
           ok ? "══ OVERALL: ALL CHECKS PASSED ══"
              : "══ OVERALL: VALIDATION FAILED  ══");

    return ok;
}

/* ─── spawn_child: fork + exec a child process ───────────────────────── */
static pid_t spawn_child(const char *exe, char *const argv[])
{
    pid_t pid = fork();
    if (pid < 0) {
        perror("controller: fork");
        return -1;
    }
    if (pid == 0) {
        /* Child: exec the target binary */
        execv(exe, argv);
        perror("controller: execv");
        _exit(EXIT_FAILURE);
    }
    return pid;   /* parent returns child PID */
}

/* ─── wait_for_all: reap all children ────────────────────────────────── */
static void wait_for_all(pid_t *pids, int count, const char *role)
{
    for (int i = 0; i < count; i++) {
        if (pids[i] <= 0) continue;
        int status;
        pid_t ret;
        /* Restart waitpid on EINTR (e.g. from signals) */
        do {
            ret = waitpid(pids[i], &status, 0);
        } while (ret < 0 && errno == EINTR);

        if (ret < 0) {
            perror("controller: waitpid");
            continue;
        }
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            LOG_W("Controller: %s[%d] (pid=%d) exited with status %d",
                  role, i, (int)pids[i], WEXITSTATUS(status));
        }
    }
}

/* ─── Main ───────────────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    /* ── Default parameters ─────────────────────────────────────────────── */
    int      buffer_size    = 0;
    int      num_producers  = 0;
    int      num_consumers  = 0;
    uint64_t total_items    = 0;
    int      urgent_pct     = 20;    /* 20% urgent by default               */
    int      do_validate    = 1;     /* validation on by default            */

    /* ── Parse positional arguments ─────────────────────────────────────── */
    if (argc < 5) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    buffer_size   = atoi(argv[1]);
    num_producers = atoi(argv[2]);
    num_consumers = atoi(argv[3]);
    total_items   = (uint64_t)atoll(argv[4]);

    /* ── Parse optional flags ───────────────────────────────────────────── */
    for (int i = 5; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return EXIT_SUCCESS;
        } else if (strcmp(argv[i], "--urgent") == 0 && i + 1 < argc) {
            urgent_pct = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no-validate") == 0) {
            do_validate = 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    /* ── Input validation ───────────────────────────────────────────────── */
    if (buffer_size < 1 || buffer_size > MAX_BUFFER_SIZE) {
        fprintf(stderr, "Error: buffer_size must be 1-%d\n", MAX_BUFFER_SIZE);
        return EXIT_FAILURE;
    }
    if (num_producers < 1 || num_producers > MAX_PRODUCERS) {
        fprintf(stderr, "Error: num_producers must be 1-%d\n", MAX_PRODUCERS);
        return EXIT_FAILURE;
    }
    if (num_consumers < 1 || num_consumers > MAX_CONSUMERS) {
        fprintf(stderr, "Error: num_consumers must be 1-%d\n", MAX_CONSUMERS);
        return EXIT_FAILURE;
    }
    if (total_items < 1 || total_items > MAX_ITEMS) {
        fprintf(stderr, "Error: total_items must be 1-%d\n", MAX_ITEMS);
        return EXIT_FAILURE;
    }
    if (urgent_pct < 0 || urgent_pct > 100) {
        fprintf(stderr, "Error: urgent percentage must be 0-100\n");
        return EXIT_FAILURE;
    }

    /* ── Ensure logs directory exists ───────────────────────────────────── */
    mkdir("logs", 0755);   /* silently ignore EEXIST */

    /* ── Initialise logging ─────────────────────────────────────────────── */
    if (log_init() != 0) {
        fprintf(stderr, "controller: log_init failed\n");
        return EXIT_FAILURE;
    }

    LOG_IS("════════════════════════════════════════════════");
    LOG_IS("Producer-Consumer Synchronization Service");
    LOG_I("  buffer_size   = %d",  buffer_size);
    LOG_I("  num_producers = %d",  num_producers);
    LOG_I("  num_consumers = %d",  num_consumers);
    LOG_I("  total_items   = %llu",(unsigned long long)total_items);
    LOG_I("  urgent_pct    = %d%%", urgent_pct);
    LOG_IS("════════════════════════════════════════════════");

    /* ── Install signal handlers ────────────────────────────────────────── */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sa.sa_flags   = SA_RESETHAND;   /* auto-reset after first delivery       */
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* ── Phase 1: Clean up any stale IPC objects from a previous run ───── */
    LOG_IS("Controller: unlinking stale IPC objects (if any)");
    shm_unlink(SHM_NAME);
    sem_unlink(SEM_EMPTY);
    sem_unlink(SEM_FULL);
    sem_unlink(SEM_MUTEX);
    sem_unlink(SEM_LOG);
    sem_unlink(SEM_PROD_DONE);

    /* ── Phase 2: Create shared memory ──────────────────────────────────── */
    int shm_fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0666);
    if (shm_fd < 0) {
        perror("controller: shm_open");
        log_close();
        return EXIT_FAILURE;
    }

    /* Size the shared memory object */
    if (ftruncate(shm_fd, sizeof(SharedBuffer)) < 0) {
        perror("controller: ftruncate");
        shm_unlink(SHM_NAME);
        log_close();
        return EXIT_FAILURE;
    }

    SharedBuffer *shm = (SharedBuffer *)mmap(NULL, sizeof(SharedBuffer),
                                              PROT_READ | PROT_WRITE,
                                              MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) {
        perror("controller: mmap");
        shm_unlink(SHM_NAME);
        close(shm_fd);
        log_close();
        return EXIT_FAILURE;
    }
    close(shm_fd);   /* fd no longer needed */
    g_shm = shm;     /* expose to signal handler */

    /* ── Phase 3: Initialise SharedBuffer ───────────────────────────────── */
    memset(shm, 0, sizeof(SharedBuffer));
    shm->buffer_size     = buffer_size;
    shm->head            = 0;
    shm->tail            = 0;
    shm->count           = 0;
    shm->urgent_head     = 0;
    shm->urgent_tail     = 0;
    shm->urgent_count    = 0;
    shm->total_produced  = 0;
    shm->total_consumed  = 0;
    shm->items_requested = total_items;
    shm->shutdown_flag   = STATUS_RUNNING;
    shm->producers_done  = 0;
    shm->num_producers   = num_producers;

    LOG_I("Controller: shared memory initialised (%zu bytes)", sizeof(SharedBuffer));

    /* ── Phase 4: Create semaphores ─────────────────────────────────────── */
    /*
     * empty  : starts at buffer_size (all slots free)
     * full   : starts at 0           (no items yet)
     * mutex  : starts at 1           (unlocked)
     */
    sem_t *sem_empty = sem_open(SEM_EMPTY, O_CREAT | O_EXCL, 0666, buffer_size);
    sem_t *sem_full  = sem_open(SEM_FULL,  O_CREAT | O_EXCL, 0666, 0);
    sem_t *sem_mutex = sem_open(SEM_MUTEX, O_CREAT | O_EXCL, 0666, 1);

    if (sem_empty == SEM_FAILED || sem_full == SEM_FAILED ||
        sem_mutex == SEM_FAILED) {
        perror("controller: sem_open");
        cleanup_ipc(shm);
        log_close();
        return EXIT_FAILURE;
    }

    LOG_I("Controller: semaphores created (empty=%d, full=0, mutex=1)", buffer_size);

    /* ── Phase 5: Determine item count per producer ─────────────────────── */
    /*
     * Distribute items as evenly as possible.
     * items_per_producer[i] = base + (i < remainder ? 1 : 0)
     */
    uint64_t base      = total_items / (uint64_t)num_producers;
    int      remainder = (int)(total_items % (uint64_t)num_producers);

    /* ── Phase 6: Spawn consumers first (they block on sem_full) ────────── */
    pid_t consumer_pids[MAX_CONSUMERS];
    memset(consumer_pids, 0, sizeof(consumer_pids));

    for (int i = 0; i < num_consumers; i++) {
        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", i);

        char *cargv[] = { "./consumer", id_str, NULL };
        consumer_pids[i] = spawn_child("./consumer", cargv);
        if (consumer_pids[i] < 0) {
            LOG_E("Controller: failed to spawn consumer %d", i);
            cleanup_ipc(shm);
            log_close();
            return EXIT_FAILURE;
        }
        LOG_I("Controller: spawned consumer %d (pid=%d)", i, (int)consumer_pids[i]);
    }

    /* Brief pause to let consumers initialise before producers start */
    usleep(50000);   /* 50 ms */

    /* ── Phase 7: Spawn producers ───────────────────────────────────────── */
    pid_t producer_pids[MAX_PRODUCERS];
    memset(producer_pids, 0, sizeof(producer_pids));

    for (int i = 0; i < num_producers; i++) {
        uint64_t my_items = base + (i < remainder ? 1 : 0);

        char id_str[16], items_str[24], upct_str[8];
        snprintf(id_str,    sizeof(id_str),    "%d", i);
        snprintf(items_str, sizeof(items_str), "%llu", (unsigned long long)my_items);
        snprintf(upct_str,  sizeof(upct_str),  "%d", urgent_pct);

        char *pargv[] = { "./producer", id_str, items_str, upct_str, NULL };
        producer_pids[i] = spawn_child("./producer", pargv);
        if (producer_pids[i] < 0) {
            LOG_E("Controller: failed to spawn producer %d", i);
            cleanup_ipc(shm);
            log_close();
            return EXIT_FAILURE;
        }
        LOG_I("Controller: spawned producer %d (pid=%d, items=%llu)",
              i, (int)producer_pids[i], (unsigned long long)my_items);
    }

    /* ── Phase 8: Wait for all children ─────────────────────────────────── */
    LOG_IS("Controller: waiting for all children...");
    wait_for_all(producer_pids, num_producers, "producer");
    LOG_I("Controller: all producers done – posting %d wake tokens for consumers",
          num_consumers);
    /*
     * Producers have all exited and shutdown_flag is set.
     * Any consumer still blocked in sem_wait(full) will never receive a
     * real item, so we post one token per consumer to unblock them.
     * Each consumer will see the empty buffer + shutdown and self-exit.
     */
    for (int i = 0; i < num_consumers; i++) {
        sem_post(sem_full);
    }
    wait_for_all(consumer_pids, num_consumers, "consumer");
    LOG_IS("Controller: all children have exited");

    /* ── Phase 9: Validation ─────────────────────────────────────────────── */
    int validation_ok = 1;
    if (do_validate) {
        validation_ok = validate(shm, total_items);
        LOG_I("Validation result: %s", validation_ok ? "PASSED" : "FAILED");
    }

    /* ── Phase 10: Summary statistics ───────────────────────────────────── */
    printf("┌─────────────────────────────────────────────┐\n");
    printf("│           EXECUTION SUMMARY                 │\n");
    printf("├─────────────────────────────────────────────┤\n");
    printf("│ buffer_size   : %-28d │\n", buffer_size);
    printf("│ num_producers : %-28d │\n", num_producers);
    printf("│ num_consumers : %-28d │\n", num_consumers);
    printf("│ total_items   : %-28llu │\n", (unsigned long long)total_items);
    printf("│ urgent_pct    : %-27d%% │\n", urgent_pct);
    printf("│ produced      : %-28llu │\n", (unsigned long long)shm->total_produced);
    printf("│ consumed      : %-28llu │\n", (unsigned long long)shm->total_consumed);
    printf("└─────────────────────────────────────────────┘\n");

    /* ── Phase 11: Cleanup ───────────────────────────────────────────────── */
    LOG_IS("Controller: cleaning up IPC resources");

    sem_close(sem_empty);
    sem_close(sem_full);
    sem_close(sem_mutex);

    cleanup_ipc(shm);
    g_shm = NULL;

    log_close();

    return validation_ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
