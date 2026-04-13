#!/bin/bash
# Interactive Raft cluster manager — run from project root: ./raft_menu.sh

BINARY="./raft_node"
CLIENT="./raft_client"
CONF3="cluster.conf"
CONF5="cluster5.conf"
CONF="$CONF3"
NODE_COUNT=3

# macOS pgrep has no -c flag
count_running() {
    pgrep -f 'raft_node [0-9]' 2>/dev/null | wc -l | tr -d ' '
}

banner() {
    clear 2>/dev/null || printf '\033[2J\033[H'
    echo "Raft Cluster Manager"
    echo ""
    local running
    running=$(count_running)
    if [ "$running" -gt 0 ]; then
        echo "  Cluster : RUNNING (${running} node(s), conf: ${CONF})"
        local leader
        leader=$(find_leader_quiet)
        if [ -n "$leader" ]; then
            echo "  Leader  : Node ${leader}"
        else
            echo "  Leader  : none detected yet"
        fi
    else
        echo "  Cluster : STOPPED  (size: ${NODE_COUNT}-node)"
    fi
}

pause() {
    echo ""
    echo "  Press Enter to return to the menu..."
    read -r
}

require_binaries() {
    local ok=1
    if [ ! -f "$BINARY" ]; then
        echo "  Error: $BINARY not found."
        ok=0
    fi
    if [ ! -f "$CLIENT" ]; then
        echo "  Warning: $CLIENT not found (client commands unavailable)."
    fi
    if [ "$ok" -eq 0 ]; then
        echo "  Run 'make' from the project root first."
        return 1
    fi
    return 0
}

require_running() {
    local running
    running=$(count_running)
    if [ "$running" -eq 0 ]; then
        echo "  No cluster is running. Start one first (option 1)."
        return 1
    fi
    return 0
}

find_leader_quiet() {
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if [ -f "node_${i}.log" ] && grep -q "Became Leader" "node_${i}.log" 2>/dev/null; then
            echo "$i"
            return
        fi
    done
}

action_choose_size() {
    echo "  Select cluster size:"
    echo "    1)  3-node  (cluster.conf)"
    echo "    2)  5-node  (cluster5.conf)"
    echo ""
    read -rp "  Choice [1/2]: " sz
    case "$sz" in
        2) CONF="$CONF5"; NODE_COUNT=5
           echo "  Switched to 5-node cluster." ;;
        *) CONF="$CONF3"; NODE_COUNT=3
           echo "  Switched to 3-node cluster." ;;
    esac
}

action_start() {
    require_binaries || return

    local running
    running=$(count_running)
    if [ "$running" -gt 0 ]; then
        echo "  Cluster already running ($running process(es))."
        echo "  Kill it first (option 2) before starting a new one."
        return
    fi

    echo "  Starting ${NODE_COUNT}-node Raft cluster (${CONF})..."
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        $BINARY "$i" "$CONF" > "node_${i}.log" 2>&1 &
        echo "  Node ${i} started  (PID $!,  log: node_${i}.log)"
    done
    echo ""
    echo "  Waiting for leader election (3 s)..."
    sleep 3

    local leader
    leader=$(find_leader_quiet)
    if [ -n "$leader" ]; then
        echo "  Leader elected: Node ${leader}"
    else
        echo "  No leader detected yet — try 'Show status' in a moment."
    fi
}

action_kill() {
    echo "  Killing all raft_node processes..."
    pkill -f 'raft_node' 2>/dev/null || true
    sleep 1
    local remaining
    remaining=$(count_running)
    if [ "$remaining" -gt 0 ]; then
        echo "  Force-killing ${remaining} stubborn process(es)..."
        pkill -9 -f 'raft_node' 2>/dev/null || true
        sleep 1
    fi
    echo "  Cluster stopped."
}

action_status() {
    require_running || return

    echo "  Cluster status (${CONF}, ${NODE_COUNT} nodes)"
    echo ""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        local log="node_${i}.log"
        local pid
        pid=$(pgrep -f "raft_node $i " 2>/dev/null | head -1)
        if [ -n "$pid" ]; then
            printf "  Node %-2s  PID %-7s" "$i" "$pid"
        else
            printf "  Node %-2s  dead       " "$i"
        fi

        if [ -f "$log" ]; then
            local is_leader=""
            grep -q "Became Leader" "$log" 2>/dev/null && is_leader="LEADER"
            local applied
            applied=$(grep -c "Applied log" "$log" 2>/dev/null || echo 0)
            local last_term
            last_term=$(grep "Became Leader\|Stepped down\|term" "$log" 2>/dev/null | tail -1 | grep -o "term [0-9]*" | tail -1)
            printf "  applied=%-4s  %-10s  %s\n" "$applied" "$is_leader" "$last_term"
        else
            echo "  (no log file)"
        fi
    done
}

action_view_logs() {
    echo "Node Logs (last 10 lines each)"
    echo ""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        local log="node_${i}.log"
        if [ -f "$log" ]; then
            echo "  -- node_${i}.log --"
            tail -10 "$log" | sed 's/^/  /'
            echo ""
        else
            echo "  node_${i}.log not found"
        fi
    done
}

action_send_put() {
    require_running || return
    if [ ! -f "$CLIENT" ]; then
        echo "  $CLIENT not found."; return
    fi

    local leader_node
    leader_node=$(find_leader_quiet)
    if [ -z "$leader_node" ]; then
        echo "  No leader detected in logs yet. Is the cluster fully started?"
        read -rp "  Enter leader node ID manually (0-$((NODE_COUNT-1))): " leader_node
    fi

    read -rp "  Key   (single letter a-z): " key
    read -rp "  Value (integer)          : " value

    if [[ ! "$key" =~ ^[a-zA-Z]$ ]] || [[ ! "$value" =~ ^-?[0-9]+$ ]]; then
        echo "  Invalid input."; return
    fi

    echo "  Sending: put ${key} ${value} to Node ${leader_node}..."
    local result
    result=$(printf "put %s %s\nquit\n" "$key" "$value" | "$CLIENT" "$CONF" "$leader_node" 2>&1)
    echo "  Response: ${result}"
}

action_send_get() {
    require_running || return
    if [ ! -f "$CLIENT" ]; then
        echo "  $CLIENT not found."; return
    fi

    local leader_node
    leader_node=$(find_leader_quiet)
    if [ -z "$leader_node" ]; then
        read -rp "  Enter leader node ID manually (0-$((NODE_COUNT-1))): " leader_node
    fi

    read -rp "  Key (single letter a-z): " key

    if [[ ! "$key" =~ ^[a-zA-Z]$ ]]; then
        echo "  Invalid key."; return
    fi

    echo "  Sending: get ${key} to Node ${leader_node}..."
    local result
    result=$(printf "get %s\nquit\n" "$key" | "$CLIENT" "$CONF" "$leader_node" 2>&1)
    echo "  Response: ${result}"
}

clear 2>/dev/null || printf '\033[2J\033[H'
echo "Raft Cluster Manager"
echo ""
echo "  Choose cluster size to start with:"
echo "    1)  3-node  (cluster.conf)"
echo "    2)  5-node  (cluster5.conf)"
echo ""
read -rp "  Choice [1/2, default 1]: " _init
case "$_init" in
    2) CONF="$CONF5"; NODE_COUNT=5 ;;
    *) CONF="$CONF3"; NODE_COUNT=3 ;;
esac
echo "  Using ${NODE_COUNT}-node cluster (${CONF})."
sleep 1

while true; do
    banner
    echo ""
    echo "  Cluster Control"
    echo "   1)  Start cluster"
    echo "   2)  Kill cluster"
    echo "   3)  Show cluster status"
    echo "   4)  View node logs (last 10 lines)"
    echo ""
    echo "  Client Operations"
    echo "   5)  Send PUT  (put key value)"
    echo "   6)  Send GET  (get key)"
    echo ""
    echo "  Settings"
    echo "   7)  Change cluster size (currently: ${NODE_COUNT}-node)"
    echo "   0)  Exit"
    echo ""
    read -rp "  Choice: " choice

    case "$choice" in
        1)  banner; action_start;        pause ;;
        2)  banner; action_kill;         pause ;;
        3)  banner; action_status;       pause ;;
        4)  banner; action_view_logs;    pause ;;
        5)  banner; action_send_put;     pause ;;
        6)  banner; action_send_get;     pause ;;
        7)  banner; action_choose_size;  pause ;;
        0)  echo "  Goodbye."; exit 0 ;;
        *)  echo "  Unknown option."; sleep 1 ;;
    esac
done
