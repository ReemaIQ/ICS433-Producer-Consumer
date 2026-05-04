# Producer–Consumer Synchronization Service

**ICS433 — Operating Systems | Group 2, Section F61**

A robust, bounded-buffer synchronization system built in **C (C11)** on Linux, using **POSIX shared memory** and **named semaphores**. Producers and consumers are fully independent processes (not threads), communicating exclusively through shared memory.

---

## Team Members

| Name | Student ID |
|------|-----------|
| Reema Ibrahim AlQahtani | 202244660 |
| Sara Azmi | 202271160 |
| Sarah Imad | 202243500 |
| Hawra Ali | 202333090 |

---

## Table of Contents

1. [Overview](#overview)
2. [Requirements](#requirements)
3. [File Structure](#file-structure)
4. [Build](#build)
5. [Usage](#usage)
6. [Architecture](#architecture)
7. [How It Works](#how-it-works)
8. [Optional Extensions](#optional-extensions)
9. [Validation Mode](#validation-mode)
10. [Logging](#logging)
11. [Makefile Targets](#makefile-targets)
12. [Cleanup](#cleanup)

---

## Overview

| Property | Value |
|---|---|
| Language | C (C11, GNU extensions) |
| IPC mechanism | POSIX shared memory (`shm_open`, `mmap`) |
| Synchronization | POSIX named semaphores (`sem_open`) |
| Process model | Controller forks N producers + M consumers |
| Priority support | Two-tier queue: URGENT items drain before NORMAL |
| Validation | Bitmap-based: detects lost and duplicate items |
| Logging | Timestamped, cross-process safe, file + stdout |

---

## Requirements

- **OS:** Linux (Ubuntu 20.04+ recommended)
- **Compiler:** GCC 9+ with C11 support
- **Libraries:** POSIX realtime (`-lrt`), POSIX threads (`-lpthread`)
- **Build tool:** GNU Make

Install on Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install build-essential
```

---

## File Structure

```
ICS433-Producer-Consumer/
├── include/
│   ├── shared.h         # Shared memory layout, Item struct, constants, macros
│   └── logger.h         # Logging API (LOG_I, LOG_W, LOG_E macros)
├── src/
│   ├── controller.c     # Entry point: init IPC, spawn processes, validate, cleanup
│   ├── producer.c       # Producer process: generate and insert items
│   ├── consumer.c       # Consumer process: remove and process items
│   └── logger.c         # Process-safe timestamped logger
├── scripts/
│   └── run.sh           # Automated test runner (12 test cases)
├── docs/
│   ├── design.md        # Architecture and design documentation
│   └── report.md        # Project report
├── logs/                # Created at runtime — pc_service.log written here
├── Makefile
├── .gitignore
└── README.md
```

---

## Build

```bash
# Build all three binaries: controller, producer, consumer
make

# Remove compiled binaries and log file
make clean
```

Compiler flags used: `-Wall -Wextra -Wpedantic -O2 -g -std=c11 -D_GNU_SOURCE`

Linked libraries: `-lrt -lpthread`

---

## Usage

```
./controller <buffer_size> <num_producers> <num_consumers> <total_items>
             [--urgent <pct>] [--no-validate] [--help]
```

| Argument | Description | Limits |
|---|---|---|
| `buffer_size` | Capacity of the shared circular buffer | 1 – 1024 |
| `num_producers` | Number of producer processes to spawn | 1 – 64 |
| `num_consumers` | Number of consumer processes to spawn | 1 – 64 |
| `total_items` | Total items to produce across all producers | 1 – 1,000,000 |
| `--urgent <pct>` | Percentage of items marked `PRIORITY_URGENT` (default 20) | 0 – 100 |
| `--no-validate` | Skip the post-run validation pass | |
| `--help` | Print usage and exit | |

### Examples

```bash
# Basic: 10-slot buffer, 3 producers, 2 consumers, 100 items
./controller 10 3 2 100

# With 30% urgent items
./controller 10 3 2 100 --urgent 30

# Stress test: 64-slot buffer, 8 producers, 4 consumers, 10000 items
./controller 64 8 4 10000

# Skip validation (useful for benchmarking)
./controller 64 8 4 10000 --no-validate
```

### Run the automated test suite

```bash
bash scripts/run.sh          # Run all 12 test cases
bash scripts/run.sh 6        # Run only test 6 (stress test)
```

---

## Architecture

```
                         ┌───────────────────────────────────┐
                         │         Shared Memory              │
                         │                                    │
  ┌──────────────┐       │  ┌─────────────────────────────┐  │       ┌──────────────┐
  │  Producer 0  │──────▶│  │  urgent_queue[]  (FIFO)     │  │──────▶│  Consumer 0  │
  │  Producer 1  │──────▶│  │  buffer[]  (circular queue) │  │──────▶│  Consumer 1  │
  │  Producer N  │──────▶│  │  head, tail, counts         │  │──────▶│  Consumer M  │
  └──────────────┘       │  │  total_produced / consumed  │  │       └──────────────┘
                         │  │  consumed_bitmap            │  │
                         │  │  shutdown_flag              │  │
                         │  └─────────────────────────────┘  │
                         └───────────────────────────────────┘
                                          │
                              Named Semaphores (POSIX)
                              ┌────────────────────────┐
                              │  /pc_sem_empty  (slots) │
                              │  /pc_sem_full   (items) │
                              │  /pc_sem_mutex  (mutex) │
                              │  /pc_sem_log    (log)   │
                              └────────────────────────┘

  Controller process: spawns all children, waits, validates, cleans up.
```

---

## How It Works

### Semaphore Protocol

Three semaphores coordinate access to the shared buffer:

| Semaphore | Initial Value | Meaning |
|---|---|---|
| `empty` | `buffer_size` | Number of free slots available |
| `full` | `0` | Number of filled slots available |
| `mutex` | `1` | Mutual exclusion on shared data |

**Producer sequence** (per item):
1. `sem_wait(empty)` — block if buffer is full
2. `sem_wait(mutex)` — enter critical section
3. Claim unique sequence number, build item, insert into buffer
4. Increment `total_produced`
5. `sem_post(mutex)` — exit critical section
6. `sem_post(full)` — signal item available to consumers

**Consumer sequence** (per item):
1. `sem_wait(full)` — block if buffer is empty
2. `sem_wait(mutex)` — enter critical section
3. Remove item (urgent queue takes priority over normal buffer)
4. Mark sequence number in `consumed_bitmap`
5. Increment `total_consumed`
6. `sem_post(mutex)` — exit critical section
7. `sem_post(empty)` — signal slot is free to producers

### Circular Buffer

```
 buffer[0..buffer_size-1]
 head ──▶ consumer reads from here
 tail ──▶ producer writes to here
 Index wrap: next = (idx + 1) % buffer_size
```

### Shutdown Sequence

1. When all producers finish, the last one sets `shutdown_flag = STATUS_SHUTDOWN` under the mutex.
2. The controller posts `num_consumers` extra tokens on `sem_full` to unblock waiting consumers.
3. Each consumer waking with an empty buffer and shutdown set exits, posting one more `sem_full` token for siblings.
4. The controller waits for all consumers, runs validation, cleans up IPC resources, and exits.

---

## Optional Extensions

### Priority Queue (Urgent Items)

Items can be flagged `PRIORITY_URGENT` (via `--urgent <pct>`).

- Urgent items go into a **separate FIFO queue** (`urgent_queue`) inside shared memory.
- Consumers always drain `urgent_queue` completely before pulling from the normal `buffer`.
- This guarantees urgent items are never delayed behind normal items.

Log output distinguishes them:

```
Producer 1 | seq=42  | priority=URGENT | slot=5 | produced
Consumer 0 | seq=42  | priority=URGENT | slot=5 | latency=152us | consumed
```

### Graceful Shutdown (SIGINT / SIGTERM)

- The controller installs handlers for `SIGINT` and `SIGTERM`.
- On receiving a signal, it sets `shutdown_flag` in shared memory.
- Producers check the flag before each production cycle and stop early.
- Consumers follow the standard empty-buffer-plus-shutdown exit path.
- All IPC resources are still cleaned up properly.

---

## Validation Mode

After all processes finish (default: on, disable with `--no-validate`), the controller runs:

1. **Count check**: `total_produced == items_requested`
2. **Completeness check**: `total_consumed == total_produced`
3. **Bitmap scan**: every sequence number `1..total_produced` must be set. Any gap = lost item.
4. **Buffer drain check**: `count == 0 && urgent_count == 0`

Sample output:

```
╔══════════════════════════════════════════════╗
║            VALIDATION REPORT                 ║
╚══════════════════════════════════════════════╝
Items requested  : 100
Items produced   : 100
Items consumed   : 100

[PASS] produced == requested (100)
[PASS] consumed == produced (100)
[PASS] No lost items detected
[PASS] Buffer is empty at end

══ OVERALL: ALL CHECKS PASSED ══
```

The controller exits with status `0` on success, `1` on validation failure.

---

## Logging

All processes log to both **stdout** and **`logs/pc_service.log`**.

Log format:

```
[YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [pid] message
```

Example entries:

```
[2026-04-22 17:02:05.920] [INFO ] [243] Producer 0 | seq=1  | val=1 | priority=NORMAL | slot=0 | produced
[2026-04-22 17:02:05.920] [INFO ] [241] Consumer 0 | seq=1  | val=1 | priority=NORMAL | slot=0 | producer=0 | latency=598us | consumed
[2026-04-22 17:02:05.926] [INFO ] [244] Producer 1 | seq=4  | val=4 | priority=URGENT | slot=0 | produced
[2026-04-22 17:02:05.927] [INFO ] [241] Consumer 0 | seq=4  | val=4 | priority=URGENT | slot=0 | producer=1 | latency=1532us | consumed
```

Logging is protected by a dedicated POSIX named semaphore (`/pc_sem_log`) so output from concurrent processes never interleaves.

---

## Makefile Targets

| Target | Description |
|---|---|
| `all` | Build `controller`, `producer`, `consumer` (default) |
| `clean` | Remove binaries, object files, and log file |
| `run` | Quick smoke test: `./controller 10 3 2 50` |
| `runbig` | Stress test: `./controller 64 8 4 10000` |
| `runurgent` | Priority test: `./controller 10 3 2 50 --urgent 40` |
| `valgrind` | Memory/resource check under valgrind |

---

## Cleanup

IPC objects created at runtime:

| Type | Name |
|---|---|
| Shared memory | `/pc_shared_buffer` |
| Semaphore | `/pc_sem_empty` |
| Semaphore | `/pc_sem_full` |
| Semaphore | `/pc_sem_mutex` |
| Semaphore | `/pc_sem_log` |

If the program crashes mid-run, clean up manually with:

```bash
rm -f /dev/shm/pc_shared_buffer
for s in pc_sem_empty pc_sem_full pc_sem_mutex pc_sem_log pc_sem_prod_done; do
    rm -f /dev/shm/sem.$s
done
```

Or simply run `./controller` again — it always unlinks stale objects at startup.
