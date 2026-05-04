/*
 * logger.c
 * --------
 * Implementation of the process-safe logging subsystem.
 *
 * Design notes:
 *  - Uses a POSIX named semaphore (SEM_LOG) as a cross-process mutex so
 *    that concurrent producer/consumer processes do not interleave output.
 *  - Each log line is written atomically: timestamp, level, PID, message.
 *  - Output goes to both stdout (for live observation) and LOG_FILE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <semaphore.h>
#include <sys/stat.h>
#include <errno.h>

#include "shared.h"
#include "logger.h"

/* ─── Module-level state ─────────────────────────────────────────────── */

static FILE *log_fp  = NULL;   /* file handle for LOG_FILE                  */
static sem_t *log_sem = NULL;  /* handle for SEM_LOG                        */

/* ─── log_init ───────────────────────────────────────────────────────── */

int log_init(void)
{
    /* Open or create the log file in append mode */
    log_fp = fopen(LOG_FILE, "a");
    if (!log_fp) {
        perror("log_init: fopen");
        return -1;
    }

    /*
     * Open or create the log semaphore.
     * O_CREAT | O_EXCL would fail if another process already created it,
     * so we use O_CREAT without O_EXCL — safe for our use case because
     * the initial value only matters on the very first creation.
     */
    log_sem = sem_open(SEM_LOG, O_CREAT, 0666, 1);
    if (log_sem == SEM_FAILED) {
        perror("log_init: sem_open SEM_LOG");
        fclose(log_fp);
        log_fp = NULL;
        return -1;
    }

    return 0;
}

/* ─── log_write ──────────────────────────────────────────────────────── */

void log_write(const char *level, const char *fmt, ...)
{
    /* Build the formatted message into a local buffer first */
    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    /* Get high-resolution timestamp */
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    struct tm tm_info;
    localtime_r(&ts.tv_sec, &tm_info);

    char time_buf[32];
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &tm_info);

    long ms = ts.tv_nsec / 1000000L;  /* milliseconds part */

    /* Acquire cross-process log mutex */
    if (log_sem) sem_wait(log_sem);

    /* Write to stdout */
    printf("[%s.%03ld] [%s] [%d] %s\n",
           time_buf, ms, level, (int)getpid(), msg);
    fflush(stdout);

    /* Write to log file */
    if (log_fp) {
        fprintf(log_fp, "[%s.%03ld] [%s] [%d] %s\n",
                time_buf, ms, level, (int)getpid(), msg);
        fflush(log_fp);
    }

    /* Release log mutex */
    if (log_sem) sem_post(log_sem);
}

/* ─── log_close ──────────────────────────────────────────────────────── */

void log_close(void)
{
    if (log_fp) {
        fclose(log_fp);
        log_fp = NULL;
    }
    if (log_sem) {
        sem_close(log_sem);
        log_sem = NULL;
    }
}
