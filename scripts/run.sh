#!/bin/bash
# =============================================================================
# run.sh — Automated Test Runner
# ICS433 Producer-Consumer Synchronization Service | Group 2, Section F61
#
# Usage:
#   bash scripts/run.sh          # Run all 12 test cases
#   bash scripts/run.sh <N>      # Run only test N (1-12)
#
# Requirements: GCC, GNU Make, Linux with POSIX realtime extensions
# =============================================================================

set -e

# Move to repo root regardless of where the script is called from
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# ── Build ────────────────────────────────────────────────────────────────────
echo "Building project..."
make -s clean
make -s all
mkdir -p logs
echo "Build successful."
echo ""

# ── Helpers ──────────────────────────────────────────────────────────────────
PASS=0
FAIL=0

run_test() {
    local num="$1"
    local name="$2"
    local cmd="$3"
    local expected_exit="${4:-0}"

    echo "──────────────────────────────────────────────────────"
    echo "  TEST $num: $name"
    echo "  CMD : $cmd"
    echo "──────────────────────────────────────────────────────"

    set +e
    eval "$cmd" 2>&1
    local actual_exit=$?
    set -e

    if [ "$actual_exit" -eq "$expected_exit" ]; then
        echo ""
        echo "  ✔  PASS  (exit $actual_exit, expected $expected_exit)"
        PASS=$((PASS + 1))
    else
        echo ""
        echo "  ✘  FAIL  (exit $actual_exit, expected $expected_exit)"
        FAIL=$((FAIL + 1))
    fi
    echo ""
}

# ── Test Cases ───────────────────────────────────────────────────────────────
TARGET="${1:-all}"

[ "$TARGET" = "all" ] || [ "$TARGET" = "1"  ] && \
  run_test  1  "Basic smoke test (10-slot, 3P, 2C, 50 items)"            "./controller 10 3 2 50"

[ "$TARGET" = "all" ] || [ "$TARGET" = "2"  ] && \
  run_test  2  "100% urgent items"                                        "./controller 10 3 2 30 --urgent 100"

[ "$TARGET" = "all" ] || [ "$TARGET" = "3"  ] && \
  run_test  3  "0% urgent items (pure normal queue)"                      "./controller 10 2 3 30 --urgent 0"

[ "$TARGET" = "all" ] || [ "$TARGET" = "4"  ] && \
  run_test  4  "Minimum buffer size (buffer=1, maximum contention)"       "./controller 1 2 2 20"

[ "$TARGET" = "all" ] || [ "$TARGET" = "5"  ] && \
  run_test  5  "Single producer / single consumer"                        "./controller 5 1 1 25"

[ "$TARGET" = "all" ] || [ "$TARGET" = "6"  ] && \
  run_test  6  "Stress test (64-slot, 8P, 4C, 1000 items)"               "./controller 64 8 4 1000"

[ "$TARGET" = "all" ] || [ "$TARGET" = "7"  ] && \
  run_test  7  "--no-validate flag (skip validation)"                     "./controller 5 2 2 20 --no-validate"

[ "$TARGET" = "all" ] || [ "$TARGET" = "8"  ] && \
  run_test  8  "--help flag"                                              "./controller --help"  1

[ "$TARGET" = "all" ] || [ "$TARGET" = "9"  ] && \
  run_test  9  "Invalid args: buffer_size=0 (should reject)"             "./controller 0 1 1 10" 1

[ "$TARGET" = "all" ] || [ "$TARGET" = "10" ] && \
  run_test 10  "30% urgent, 40 items"                                     "./controller 10 3 2 40 --urgent 30"

[ "$TARGET" = "all" ] || [ "$TARGET" = "11" ] && \
  run_test 11  "More consumers than producers (2P, 8C)"                   "./controller 8 2 8 50"

[ "$TARGET" = "all" ] || [ "$TARGET" = "12" ] && \
  run_test 12  "More producers than consumers (8P, 2C)"                   "./controller 8 8 2 50"

# ── Summary ──────────────────────────────────────────────────────────────────
if [ "$TARGET" = "all" ]; then
    echo "======================================================"
    echo "  RESULTS:  $PASS passed   |   $FAIL failed"
    echo "======================================================"
    [ "$FAIL" -eq 0 ] && exit 0 || exit 1
fi
