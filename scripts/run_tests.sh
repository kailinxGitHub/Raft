#!/bin/bash
# run_tests.sh — Build and run the full Raft test suite, then print a
# combined pass/fail report.
#
# Run from the project root:  ./scripts/run_tests.sh
# Or from within scripts/:    bash run_tests.sh  (cds to project root)

# ── colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

# ── cd to project root ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT" || { echo "Cannot cd to project root"; exit 1; }

# ── suite definitions ─────────────────────────────────────────────────────────
# Each entry: "binary_target|binary_name|friendly_name"
SUITES=(
    "test_unit|./test_unit|Unit Tests"
    "test_integration|./test_integration|Integration Tests"
    "test_advanced|./test_advanced|Advanced Tests"
    "test_edge|./test_edge|Edge Case Tests"
    "test_snapshot|./test_snapshot|Snapshot Tests"
    "test_stress|./test_stress|Stress Tests"
)

# ── build ─────────────────────────────────────────────────────────────────────
header() {
    echo -e "${CYAN}${BOLD}"
    echo "  ╔══════════════════════════════════════════════╗"
    echo "  ║       Raft Full Test Suite Runner            ║"
    echo "  ║       CS 4730 — Distributed Systems          ║"
    echo "  ╚══════════════════════════════════════════════╝"
    echo -e "${NC}"
}

header

echo -e "${BOLD}  [1/2] Building test binaries...${NC}"
echo ""

# ── hard reset before we touch anything ──────────────────────────────────────
# Kill every raft_node process regardless of how it was launched (macOS strips
# the leading './' from argv[0], so match without it).
_kill_nodes() {
    pkill -f 'raft_node' 2>/dev/null || true
    sleep 0.8
    local still
    still=$(pgrep -f 'raft_node [0-9]' 2>/dev/null | wc -l | tr -d ' ')
    if [ "$still" -gt 0 ]; then
        pkill -9 -f 'raft_node' 2>/dev/null || true
        sleep 0.5
    fi
}
_kill_nodes
rm -f test_node_*.log node_*.log snapshot_*.dat raft_state_*.dat 2>/dev/null

BUILD_FAILED=()

# Build raft_node first — integration/snapshot/stress tests launch it as a subprocess
printf "  %-14s ... " "raft_node"
if make raft_node >/dev/null 2>&1; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    BUILD_FAILED+=("raft_node (server binary)")
fi

for entry in "${SUITES[@]}"; do
    target="${entry%%|*}"
    friendly="${entry##*|}"
    printf "  %-14s ... " "$target"
    if make "$target" >/dev/null 2>&1; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        BUILD_FAILED+=("$friendly")
    fi
done

if [ "${#BUILD_FAILED[@]}" -gt 0 ]; then
    echo ""
    echo -e "${RED}  Build failed for:${NC}"
    for f in "${BUILD_FAILED[@]}"; do echo "    - $f"; done
    echo ""
    echo -e "  Run ${BOLD}make${NC} manually to see compiler errors."
    exit 1
fi

echo ""
echo -e "${BOLD}  [2/2] Running test suites...${NC}"
echo ""

# ── run suites ────────────────────────────────────────────────────────────────
# Track aggregate results
TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_TESTS=0

# Table rows collected during run
declare -a TABLE_ROWS

SUITE_IDX=0
for entry in "${SUITES[@]}"; do
    target="${entry%%|*}"
    rest="${entry#*|}"
    binary="${rest%%|*}"
    friendly="${rest##*|}"

    SUITE_IDX=$((SUITE_IDX + 1))

    echo -e "${BOLD}  ── ${friendly} ──${NC}"

    # Clean up leftover node logs / snapshots before each suite
    _kill_nodes
    rm -f test_node_*.log node_*.log snapshot_*.dat raft_state_*.dat 2>/dev/null

    # Run the binary, capturing all output
    local_out=$("$binary" 2>&1)
    local_exit=$?

    # Echo output indented
    echo "$local_out" | sed 's/^/  /'
    echo ""

    # Parse "X/Y passed" from the last summary line
    summary_line=$(echo "$local_out" | grep -E "Results: [0-9]+/[0-9]+ passed" | tail -1)
    if [ -n "$summary_line" ]; then
        passed=$(echo "$summary_line" | grep -o '[0-9]*/[0-9]*' | cut -d/ -f1)
        total=$(echo "$summary_line"  | grep -o '[0-9]*/[0-9]*' | cut -d/ -f2)
        failed=$((total - passed))
    else
        # Fallback: if binary exited 0 assume all passed (no count available)
        passed="?"
        total="?"
        failed=0
        if [ "$local_exit" -ne 0 ]; then failed=1; fi
    fi

    if [ "$passed" != "?" ]; then
        TOTAL_PASS=$((TOTAL_PASS + passed))
        TOTAL_FAIL=$((TOTAL_FAIL + failed))
        TOTAL_TESTS=$((TOTAL_TESTS + total))
    fi

    if [ "$local_exit" -eq 0 ]; then
        status="${GREEN}PASS${NC}"
    else
        status="${RED}FAIL${NC}"
    fi

    TABLE_ROWS+=("$(printf "  %-28s  %5s / %-5s  %b" "$friendly" "$passed" "$total" "$status")")

    # Clean up after each suite
    _kill_nodes
    rm -f test_node_*.log node_*.log snapshot_*.dat raft_state_*.dat 2>/dev/null
done

# ── summary table ─────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}${BOLD}  ╔══════════════════════════════════════════════════════════╗"
echo -e "  ║                    FINAL REPORT                          ║"
echo -e "  ╠══════════════════════════════════════════════════════════╣"
printf   "  ║  %-28s  %13s  %-6s ║\n" "Suite" "Passed/Total" "Result"
echo -e "  ╠══════════════════════════════════════════════════════════╣${NC}"

for row in "${TABLE_ROWS[@]}"; do
    echo -e "${row}"
done

echo -e "${CYAN}${BOLD}  ╠══════════════════════════════════════════════════════════╣"
printf   "  ║  %-28s  %5d / %-5d  " "TOTAL" "$TOTAL_PASS" "$TOTAL_TESTS"
if [ "$TOTAL_FAIL" -eq 0 ]; then
    printf "%b ║\n" "${GREEN}ALL PASS${NC}${CYAN}${BOLD}"
else
    printf "%b ║\n" "${RED}${TOTAL_FAIL} FAILED${NC}${CYAN}${BOLD}"
fi
echo -e "  ╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ "$TOTAL_FAIL" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}All tests passed.${NC}"
    exit 0
else
    echo -e "  ${RED}${BOLD}${TOTAL_FAIL} test(s) failed.${NC} See output above for details."
    exit 1
fi
