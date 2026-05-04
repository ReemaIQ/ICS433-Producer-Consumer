# System Design Document
## ICS433 — Producer-Consumer Synchronization Service
### Group 2, Section F61

---

## System Architecture

The system consists of four executables communicating exclusively via POSIX IPC:

```
                     +-----------------------------------------------+
                     |              Shared Memory                    |
  +-----------+      |  +---------------------------------------+    |      +------------+
  | Producer 0|----->|  | urgent_queue[]  (priority FIFO)      |    |----->| Consumer 0 |
  | Producer 1|----->|  | buffer[]        (normal circular buf) |    |----->| Consumer 1 |
  | Producer N|----->|  | head, tail, count, urgent_count       |    |----->| Consumer M |
  +-----------+      |  | total_produced / total_consumed        |    |      +------------+
                     |  | consumed_bitmap                       |    |
                     |  | shutdown_flag, producers_done         |    |
                     |  +---------------------------------------+    |
                     +-----------------------------------------------+
                                     Named Semaphores (POSIX)
                           /pc_sem_empty, /pc_sem_full, /pc_sem_mutex, /pc_sem_log
```

---

## Shared Memory Layout

`SharedBuffer` struct mapped via `shm_open` + `mmap`:

| Field | Type | Purpose |
|---|---|---|
| buffer[MAX_BUFFER_SIZE] | Item[] | Normal circular queue |
| urgent_queue[MAX_BUFFER_SIZE] | Item[] | Urgent priority FIFO |
| head, tail, count | int | Normal queue state |
| urgent_head, urgent_tail, urgent_count | int | Urgent queue state |
| total_produced, total_consumed, items_requested | uint64_t | Accounting |
| consumed_bitmap | uint8_t[] | Loss/duplicate detection |
| shutdown_flag | int | STATUS_RUNNING / STATUS_SHUTDOWN |
| producers_done, num_producers | int | Shutdown coordination |

---

## Circular Buffer Design

```
buffer[0 .. buffer_size-1]
head ──> consumer reads from here
tail ──> producer writes to here
wrap:    next = (idx + 1) % buffer_size
```

---

## Semaphore Protocol

| Semaphore | Init | Meaning |
|---|---|---|
| sem_empty | buffer_size | Free slots. Producers wait here when full. |
| sem_full | 0 | Filled slots. Consumers wait here when empty. |
| sem_mutex | 1 | Binary mutex on all shared state. |
| sem_log | 1 | Binary mutex on log output. |

### Producer Steps (per item):
1. Check shutdown_flag
2. sem_wait(empty)
3. sem_wait(mutex)
4. Claim seq_num, build item, insert
5. Increment total_produced
6. sem_post(mutex)
7. sem_post(full)

### Consumer Steps (per item):
1. sem_wait(full)
2. sem_wait(mutex)
3. Check exit condition
4. Remove from urgent_queue first, then buffer
5. Mark bitmap, increment total_consumed
6. sem_post(mutex)
7. sem_post(empty)

---

## Priority Queue Design

Two parallel bounded FIFOs inside shared memory:
- `urgent_queue` for PRIORITY_URGENT items
- `buffer` for PRIORITY_NORMAL items

Consumers drain `urgent_queue` to zero before touching `buffer`.
`sem_empty` counts combined capacity of both queues.

---

## Shutdown Sequence

1. Last producer sets `shutdown_flag = STATUS_SHUTDOWN` under mutex.
2. Controller posts N (num_consumers) tokens on `sem_full`.
3. Each consumer that exits posts one additional `sem_full` token (cascade).
4. Controller waits for all consumers → validates → cleans up IPC.

---

## Validation (Bitmap Scan)

After all processes finish:
1. total_produced == items_requested
2. total_consumed == total_produced
3. Every bit 1..total_produced is set in consumed_bitmap
4. count == 0 and urgent_count == 0
