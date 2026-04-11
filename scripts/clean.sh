#!/bin/bash
# clean.sh — Full project cleanup
# Kills any running cluster, removes all build artifacts, logs, and temp files.
# Run from the project root: ./scripts/clean.sh

# ── colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT" || { echo "Cannot cd to project root"; exit 1; }

echo -e "${CYAN}${BOLD}"
echo "  ╔══════════════════════════════════════════════╗"
echo "  ║            Raft Project Cleanup              ║"
echo "  ╚══════════════════════════════════════════════╝"
echo -e "${NC}"

removed=0

step() { echo -e "  ${BOLD}$1${NC}"; }
ok()   { echo -e "    ${GREEN}✓${NC}  $1"; }
skip() { echo -e "    ${DIM}–  $1${NC}"; }

# ── 1. Kill running cluster ───────────────────────────────────────────────────
step "Stopping any running raft_node processes..."
running=$(pgrep -c -f "./raft_node" 2>/dev/null || echo 0)
if [ "$running" -gt 0 ]; then
    pkill -f "./raft_node" 2>/dev/null || true
    sleep 0.5
    # Force-kill stragglers
    still=$(pgrep -c -f "./raft_node" 2>/dev/null || echo 0)
    if [ "$still" -gt 0 ]; then
        pkill -9 -f "./raft_node" 2>/dev/null || true
    fi
    ok "Killed $running process(es)"
else
    skip "No running processes"
fi
echo ""

# ── 2. Compiled binaries ──────────────────────────────────────────────────────
step "Removing compiled binaries..."
for f in raft_node raft_client \
          test_unit test_integration test_advanced test_edge test_snapshot test_stress; do
    if [ -f "$f" ]; then
        rm -f "$f"
        ok "$f"
        removed=$((removed + 1))
    else
        skip "$f"
    fi
done
echo ""

# ── 3. macOS debug symbol bundles (.dSYM) ─────────────────────────────────────
step "Removing .dSYM debug bundles..."
found=0
for d in *.dSYM; do
    [ -e "$d" ] || continue
    rm -rf "$d"
    ok "$d"
    removed=$((removed + 1))
    found=1
done
[ "$found" -eq 0 ] && skip "none found"
echo ""

# ── 4. Node runtime logs ──────────────────────────────────────────────────────
step "Removing node log files..."
found=0
for f in node_*.log test_node_*.log; do
    [ -e "$f" ] || continue
    rm -f "$f"
    ok "$f"
    removed=$((removed + 1))
    found=1
done
[ "$found" -eq 0 ] && skip "none found"
echo ""

# ── 5. Snapshot data files ────────────────────────────────────────────────────
step "Removing snapshot files..."
found=0
for f in snapshot_*.dat; do
    [ -e "$f" ] || continue
    rm -f "$f"
    ok "$f"
    removed=$((removed + 1))
    found=1
done
[ "$found" -eq 0 ] && skip "none found"
echo ""

# ── 6. Compiler temp / object files ──────────────────────────────────────────
step "Removing object and temp files..."
found=0
for f in *.o *.d src/*.o src/*.d; do
    [ -e "$f" ] || continue
    rm -f "$f"
    ok "$f"
    removed=$((removed + 1))
    found=1
done
[ "$found" -eq 0 ] && skip "none found"
echo ""

# ── summary ───────────────────────────────────────────────────────────────────
echo -e "  ${GREEN}${BOLD}Done.${NC}  Removed ${removed} item(s). Project is clean."
echo ""
