/*
 * logger.h
 * --------
 * Public API for the process-safe logging subsystem.
 *
 * All processes (controller, producer, consumer) include this header.
 * The implementation is in src/logger.c.
 */

#ifndef LOGGER_H
#define LOGGER_H

/* ─── Logging API ───────────────────────────────────────────────────────── */

/*
 * log_init  – open log file + log semaphore.  Call once per process.
 * Returns 0 on success, -1 on failure.
 */
int  log_init(void);

/*
 * log_write – write a timestamped, level-tagged log line.
 * Protected by the cross-process log semaphore.
 * Do not call directly; use the macros below.
 */
void log_write(const char *level, const char *fmt, ...);

/*
 * log_close – flush and close the log file + semaphore handle.
 * Call once per process before exit.
 */
void log_close(void);

/* ─── Convenience macros ─────────────────────────────────────────────────
 *
 *  LOG_I   – INFO  level (normal operational messages)
 *  LOG_W   – WARN  level (unexpected but non-fatal)
 *  LOG_E   – ERROR level (serious errors)
 *  LOG_IS  – INFO  level, single string (no format args needed)
 */
#define LOG_I(fmt, ...)   log_write("INFO ", fmt, ##__VA_ARGS__)
#define LOG_W(fmt, ...)   log_write("WARN ", fmt, ##__VA_ARGS__)
#define LOG_E(fmt, ...)   log_write("ERROR", fmt, ##__VA_ARGS__)
#define LOG_IS(str)       log_write("INFO ", "%s", str)

#endif /* LOGGER_H */
