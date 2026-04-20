//
// Created by Reema AL-Qahtani on 14/04/2026.
//

/* Contains:
* Main program entry
* Parse arguments
* Create shared memory
* Initialize semaphores
* Spawn processes
* Cleanup
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <semaphore.h>
#include <errno.h>
#include "shared.h"
#include "logger.h"

/*
 * controller.c
 * Usage: ./controller <buffer_size> <num_producers> <num_consumers> <total_items>
 * Example: ./controller 10 3 2 100
 */



/* ── helpers ──────────────────────────────────────────────── */
static void cleanup(SharedBuffer *shm, int shm_fd)
{
    /* detach and unlink shared memory */
    munmap(shm, sizeof(SharedBuffer));
    close(shm_fd);
    shm_unlink(SHM_NAME);

    /* close and unlink semaphores */
    sem_t *s;
    s = sem_open(SEM_EMPTY, 0); if (s != SEM_FAILED) { sem_close(s); }
    s = sem_open(SEM_FULL,  0); if (s != SEM_FAILED) { sem_close(s); }
    s = sem_open(SEM_MUTEX, 0); if (s != SEM_FAILED) { sem_close(s); }
    sem_unlink(SEM_EMPTY);
    sem_unlink(SEM_FULL);
    sem_unlink(SEM_MUTEX);
}


/* ── main ─────────────────────────────────────────────────── */
int main(int argc, char *argv[])
{
    if (argc != 5) {
        fprintf(stderr,
                "Usage: %s <buffer_size> <num_producers> <num_consumers> <total_items>\n",
                argv[0]);
        return EXIT_FAILURE;
    }

    int buf_size     = atoi(argv[1]);
    int num_prod     = atoi(argv[2]);
    int num_cons     = atoi(argv[3]);
    int total_items  = atoi(argv[4]);

    if (buf_size < 1 || buf_size > MAX_BUFFER_SIZE ||
        num_prod < 1 || num_cons < 1 || total_items < 1) {
        fprintf(stderr, "Invalid arguments.\n");
        return EXIT_FAILURE;
    }

    printf("[Controller] Starting: buf=%d producers=%d consumers=%d items=%d\n",
           buf_size, num_prod, num_cons, total_items);

    /* ── 1. Create shared memory ─────────────────────────── */
    shm_unlink(SHM_NAME);   /* remove stale objects if any */
    int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) { perror("shm_open"); return EXIT_FAILURE; }

    if (ftruncate(shm_fd, sizeof(SharedBuffer)) < 0) {
        perror("ftruncate"); return EXIT_FAILURE;
    }

    SharedBuffer *shm = mmap(NULL, sizeof(SharedBuffer),
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) { perror("mmap"); return EXIT_FAILURE; }

    /* initialise shared state */
    memset(shm, 0, sizeof(SharedBuffer));
    shm->buffer_size  = buf_size;
    shm->target_items = total_items;
    shm->done         = 0;

    /* ── 2. Create semaphores ─────────────────────────────── */
    sem_unlink(SEM_EMPTY);
    sem_unlink(SEM_FULL);
    sem_unlink(SEM_MUTEX);

    sem_t *sem_empty = sem_open(SEM_EMPTY, O_CREAT, 0666, buf_size);
    sem_t *sem_full  = sem_open(SEM_FULL,  O_CREAT, 0666, 0);
    sem_t *sem_mutex = sem_open(SEM_MUTEX, O_CREAT, 0666, 1);

    if (sem_empty == SEM_FAILED || sem_full == SEM_FAILED || sem_mutex == SEM_FAILED) {
        perror("sem_open"); cleanup(shm, shm_fd); return EXIT_FAILURE;
    }

    sem_close(sem_empty);
    sem_close(sem_full);
    sem_close(sem_mutex);

    /* ── 3. Spawn producers and consumers ─────────────────── */
    pid_t *prod_pids = malloc(num_prod * sizeof(pid_t));
    pid_t *cons_pids = malloc(num_cons * sizeof(pid_t));

    /* Build args: producer <id> <total_items> */
    char prod_id_str[16], items_str[16];
    snprintf(items_str, sizeof(items_str), "%d", total_items);

    for (int i = 0; i < num_prod; i++) {
        snprintf(prod_id_str, sizeof(prod_id_str), "%d", i);
        char *args[] = { "./producer", prod_id_str, items_str, NULL };
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); exit(EXIT_FAILURE); }
        if (pid == 0) { execvp("./producer", args); perror("execvp"); exit(EXIT_FAILURE); }
        prod_pids[i] = pid;
    }

    /* Build args: consumer <id> <total_items> <num_consumers> */
    char cons_id_str[16], num_cons_str[16];
    snprintf(num_cons_str, sizeof(num_cons_str), "%d", num_cons);

    for (int i = 0; i < num_cons; i++) {
        snprintf(cons_id_str, sizeof(cons_id_str), "%d", i);
        char *args[] = { "./consumer", cons_id_str, items_str, num_cons_str, NULL };
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); exit(EXIT_FAILURE); }
        if (pid == 0) { execvp("./consumer", args); perror("execvp"); exit(EXIT_FAILURE); }
        cons_pids[i] = pid;
    }

    /* ── 4. Wait for all children ─────────────────────────── */
    for (int i = 0; i < num_prod; i++) waitpid(prod_pids[i], NULL, 0);
    for (int i = 0; i < num_cons; i++) waitpid(cons_pids[i], NULL, 0);

    /* ── 5. Validate ──────────────────────────────────────── */
    printf("\n[Controller] === Validation ===\n");
    printf("  Target items   : %d\n", total_items);
    printf("  Total produced : %d\n", shm->total_produced);
    printf("  Total consumed : %d\n", shm->total_consumed);

    if (shm->total_produced == total_items &&
        shm->total_consumed == total_items) {
        printf("  Result         : PASS ✓\n");
    } else {
        printf("  Result         : FAIL ✗\n");
    }

    /* ── 6. Cleanup ───────────────────────────────────────── */
    cleanup(shm, shm_fd);
    free(prod_pids);
    free(cons_pids);
    printf("[Controller] Done. All IPC resources released.\n");
    return EXIT_SUCCESS;
}
