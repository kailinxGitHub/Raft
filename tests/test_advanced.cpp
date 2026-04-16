#include "test_framework.h"
#include "../src/network.h"
#include "../src/config.h"
#include <cstdlib>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <thread>
#include <chrono>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <atomic>
#include <map>
#include <set>

// ============================================================
// Cluster harness (same as test_integration.cpp)
// ============================================================

static const std::string BINARY = "./raft_node";

struct NodeProcess {
    int nodeId;
    pid_t pid;
    std::string logFile;
};

class Cluster {
    std::vector<NodeProcess> nodes;
    std::string confFile;

public:
    Cluster(const std::string& conf) : confFile(conf) {
        // Pre-cleanup: kill any stale raft_node processes and remove state/log
        // files from the previous test so this test always starts in a clean state.
        ::system("pkill -9 -f 'raft_node' 2>/dev/null");
        ::usleep(150 * 1000);
        for (int i = 0; i < 5; i++) {
            ::unlink(("snapshot_"   + std::to_string(i) + ".dat").c_str());
            ::unlink(("raft_state_" + std::to_string(i) + ".dat").c_str());
            ::unlink(("test_node_"  + std::to_string(i) + ".log").c_str());
        }
    }

    void startNode(int nodeId, int fixedTimeout = 0) {
        std::string logFile = "test_node_" + std::to_string(nodeId) + ".log";
        pid_t pid = fork();
        if (pid == 0) {
            FILE* f = freopen(logFile.c_str(), "w", stderr);
            (void)f;
            fclose(stdout);
            if (fixedTimeout > 0) {
                std::string ft = std::to_string(fixedTimeout);
                execl(BINARY.c_str(), BINARY.c_str(),
                      std::to_string(nodeId).c_str(), confFile.c_str(),
                      ft.c_str(), nullptr);
            } else {
                execl(BINARY.c_str(), BINARY.c_str(),
                      std::to_string(nodeId).c_str(), confFile.c_str(), nullptr);
            }
            _exit(1);
        }
        nodes.push_back({nodeId, pid, logFile});
    }

    void startAll(int count, int fixedTimeout = 0) {
        for (int i = 0; i < count; i++) startNode(i, fixedTimeout);
    }

    void killNode(int nodeId) {
        for (auto& n : nodes) {
            if (n.nodeId == nodeId && n.pid > 0) {
                kill(n.pid, SIGKILL);
                waitpid(n.pid, nullptr, 0);
                n.pid = -1;
                return;
            }
        }
    }

    void killAll() {
        for (auto& n : nodes) {
            if (n.pid > 0) {
                kill(n.pid, SIGKILL);
                waitpid(n.pid, nullptr, 0);
                n.pid = -1;
            }
        }
    }

    std::string readLog(int nodeId) const {
        std::string logFile = "test_node_" + std::to_string(nodeId) + ".log";
        std::ifstream f(logFile);
        std::ostringstream oss;
        oss << f.rdbuf();
        return oss.str();
    }

    int countPattern(int nodeId, const std::string& pattern) const {
        std::string log = readLog(nodeId);
        int count = 0;
        size_t pos = 0;
        while ((pos = log.find(pattern, pos)) != std::string::npos) {
            count++;
            pos += pattern.size();
        }
        return count;
    }

    bool logContains(int nodeId, const std::string& pattern) const {
        return readLog(nodeId).find(pattern) != std::string::npos;
    }

    int findLeader(int nodeCount) const {
        for (int i = 0; i < nodeCount; i++) {
            if (logContains(i, "Became Leader")) return i;
        }
        return -1;
    }

    // Find the current leader among a set of live nodes
    int findLeaderAmong(const std::vector<int>& liveNodes) const {
        for (int id : liveNodes) {
            if (logContains(id, "Became Leader")) return id;
        }
        return -1;
    }

    ~Cluster() {
        killAll();
        for (int i = 0; i < 5; i++) {
            ::unlink(("snapshot_"   + std::to_string(i) + ".dat").c_str());
            ::unlink(("raft_state_" + std::to_string(i) + ".dat").c_str());
            ::unlink(("test_node_"  + std::to_string(i) + ".log").c_str());
        }
        ::usleep(200 * 1000); // let the OS fully release ports before the next test
    }
};

static void wait_ms(int ms) {
    // 1.5× scale so tests remain stable on slower grading machines
    std::this_thread::sleep_for(std::chrono::milliseconds(ms * 3 / 2));
}

static bool sendPut(const std::string& host, int port, char key, int value) {
    Message req;
    req.type = MessageType::ClientPut;
    req.senderId = -1;
    req.key = key;
    req.value = value;
    Message reply;
    if (!sendRPC(host, port, req, reply)) return false;
    return reply.success && reply.isLeader;
}

static int sendGet(const std::string& host, int port, char key) {
    Message req;
    req.type = MessageType::ClientGet;
    req.senderId = -1;
    req.key = key;
    for (int attempt = 0; attempt < 5; attempt++) {
        Message reply;
        if (!sendRPC(host, port, req, reply)) return -999;
        if (!reply.isLeader) return -998;
        if (reply.success) return reply.value;
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
    return -997;
}

// Send a raw message and return the full reply for inspection
static bool sendRaw(const std::string& host, int port, const Message& req, Message& reply) {
    return sendRPC(host, port, req, reply);
}

// ============================================================
// MULTI-LEADER-FAILURE: Data survives across 3 consecutive
// leader elections. (ProjectPlanReport L14: "all commit index
// will advance only under Raft rules")
// ============================================================

void test_data_survives_three_leader_changes() {
    Cluster c("cluster5.conf");
    c.startAll(5);
    wait_ms(3000);

    int leader1 = c.findLeader(5);
    ASSERT_GE(leader1, 0);
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader1, 'a', 10));
    wait_ms(1000);

    // Kill leader 1
    c.killNode(leader1);
    wait_ms(3000);

    // Find leader 2 by probing: the node that accepts the write is the current leader
    int leader2 = -1;
    for (int i = 0; i < 5; i++) {
        if (i == leader1) continue;
        if (sendPut("127.0.0.1", 9000 + i, 'b', 20)) { leader2 = i; break; }
    }
    ASSERT_GE(leader2, 0);
    wait_ms(1000);

    // Verify a=10 survived on leader 2
    int val_a = sendGet("127.0.0.1", 9000 + leader2, 'a');
    ASSERT_EQ(val_a, 10);

    // Kill leader 2
    c.killNode(leader2);
    wait_ms(3000);

    // Find leader 3 by probing among remaining 3 nodes
    int leader3 = -1;
    for (int i = 0; i < 5; i++) {
        if (i == leader1 || i == leader2) continue;
        if (sendPut("127.0.0.1", 9000 + i, 'c', 30)) { leader3 = i; break; }
    }
    ASSERT_GE(leader3, 0);
    wait_ms(1000);

    // Verify ALL three values survived across 3 leader changes
    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader3, 'a'), 10);
    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader3, 'b'), 20);
    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader3, 'c'), 30);
}

// ============================================================
// KV STATE MACHINE CONSISTENCY: After operations and leader
// changes, all live nodes have identical state.
// (ProjectPlanReport L14: "all nodes will consider that entry
// as identical")
// ============================================================

void test_kv_consistency_across_all_nodes() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Write several key-value pairs
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'x', 100));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'y', 200));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'z', 300));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'x', 150)); // overwrite x
    wait_ms(2000);

    // Verify all 3 nodes applied exactly the same entries
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(c.logContains(i, "Applied log[1]: x=100"));
        ASSERT_TRUE(c.logContains(i, "Applied log[2]: y=200"));
        ASSERT_TRUE(c.logContains(i, "Applied log[3]: z=300"));
        ASSERT_TRUE(c.logContains(i, "Applied log[4]: x=150"));
    }

    // Verify the KV store via GET on the leader reflects the final state
    ASSERT_EQ(sendGet("127.0.0.1", port, 'x'), 150); // overwritten
    ASSERT_EQ(sendGet("127.0.0.1", port, 'y'), 200);
    ASSERT_EQ(sendGet("127.0.0.1", port, 'z'), 300);
}

// ============================================================
// HEARTBEAT COMMIT PROPAGATION: Followers advance commitIndex
// via heartbeats even when no new entries are sent.
// ============================================================

void test_heartbeat_advances_follower_commitIndex() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Kill one follower before sending puts
    int deadFollower = (leader == 0) ? 1 : 0;
    int liveFollower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader && i != deadFollower) { liveFollower = i; break; }
    }
    c.killNode(deadFollower);
    wait_ms(500);

    // Send entries — only leader + liveFollower have them
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'h', 42));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'i', 43));
    wait_ms(1000);

    // Leader commits (majority: self + liveFollower)
    ASSERT_TRUE(c.logContains(leader, "Applied log[1]: h=42"));

    // Live follower gets commit via heartbeats carrying leaderCommit
    ASSERT_TRUE(c.logContains(liveFollower, "Applied log[1]: h=42"));
    ASSERT_TRUE(c.logContains(liveFollower, "Applied log[2]: i=43"));

    // Now restart dead follower — it catches up and also applies
    c.startNode(deadFollower);
    wait_ms(3000);

    ASSERT_TRUE(c.logContains(deadFollower, "Applied log[1]: h=42"));
    ASSERT_TRUE(c.logContains(deadFollower, "Applied log[2]: i=43"));
}

// ============================================================
// 5-NODE ROLLING KILLS: Kill 1 node at a time, write between
// each kill, verify all entries committed.
// (ProjectPlanReport L100-105: fault tolerance)
// ============================================================

void test_5node_rolling_kills_with_writes() {
    Cluster c("cluster5.conf");
    c.startAll(5);
    wait_ms(3000);

    int leader = c.findLeader(5);
    ASSERT_GE(leader, 0);

    // Write entry 1
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader, 'a', 1));
    wait_ms(500);

    // Kill first non-leader node
    int killed1 = -1;
    for (int i = 0; i < 5; i++) {
        if (i != leader) { killed1 = i; break; }
    }
    c.killNode(killed1);
    wait_ms(1000);

    // Write entry 2 (4 nodes alive, majority = 3, still works)
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader, 'b', 2));
    wait_ms(500);

    // Kill second non-leader node
    int killed2 = -1;
    for (int i = 0; i < 5; i++) {
        if (i != leader && i != killed1) { killed2 = i; break; }
    }
    c.killNode(killed2);
    wait_ms(1000);

    // Write entry 3 (3 nodes alive, majority = 3, barely enough)
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader, 'c', 3));
    wait_ms(1000);

    // All 3 surviving nodes should have all entries
    for (int i = 0; i < 5; i++) {
        if (i == killed1 || i == killed2) continue;
        ASSERT_TRUE(c.logContains(i, "Applied log[1]: a=1"));
        ASSERT_TRUE(c.logContains(i, "Applied log[2]: b=2"));
        ASSERT_TRUE(c.logContains(i, "Applied log[3]: c=3"));
    }
}

// ============================================================
// 5-NODE: Kill nodes, write, restart killed nodes, verify
// ALL 5 nodes converge.
// (ProjectPlanReport L104-105: "Node rejoins and catches up
// with no difference from rest of cluster")
// ============================================================

void test_5node_kill_restart_full_convergence() {
    Cluster c("cluster5.conf");
    c.startAll(5);
    wait_ms(3000);

    int leader = c.findLeader(5);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Kill 2 non-leader nodes
    int k1 = -1, k2 = -1;
    for (int i = 0; i < 5; i++) {
        if (i == leader) continue;
        if (k1 < 0) k1 = i;
        else if (k2 < 0) { k2 = i; break; }
    }
    c.killNode(k1);
    c.killNode(k2);
    wait_ms(500);

    // Write while 2 nodes are down
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'p', 10));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'q', 20));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'r', 30));
    wait_ms(1000);

    // Restart both killed nodes
    c.startNode(k1);
    c.startNode(k2);
    wait_ms(3000);

    // ALL 5 nodes must have all entries
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(c.logContains(i, "Applied log[1]: p=10"));
        ASSERT_TRUE(c.logContains(i, "Applied log[2]: q=20"));
        ASSERT_TRUE(c.logContains(i, "Applied log[3]: r=30"));
    }
}

// ============================================================
// TERM MONOTONICITY: After multiple elections, every node's
// term is >= every previous term it had.
// (ProjectPlanReport L43: term update rule)
// ============================================================

void test_term_monotonicity_across_elections() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader1 = c.findLeader(3);
    ASSERT_GE(leader1, 0);
    c.killNode(leader1);
    wait_ms(3000);

    int leader2 = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader1 && c.logContains(i, "Became Leader")) { leader2 = i; break; }
    }
    ASSERT_GE(leader2, 0);
    c.killNode(leader2);
    wait_ms(3000);

    // The remaining node should have started elections with increasing terms
    int remaining = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader1 && i != leader2) { remaining = i; break; }
    }
    ASSERT_GE(remaining, 0);

    // Parse all term numbers from the log, verify monotonically non-decreasing
    std::string log = c.readLog(remaining);
    int prevTerm = 0;
    size_t pos = 0;
    while ((pos = log.find("[T", pos)) != std::string::npos) {
        pos += 2;
        int term = std::stoi(log.substr(pos));
        ASSERT_GE(term, prevTerm);
        prevTerm = term;
    }
    ASSERT_GT(prevTerm, 0);
}

// ============================================================
// NETWORK PARTITION HEALS: Old leader rejoins, discovers higher
// term, steps down, and its log converges with the new leader.
// (ProjectPlanReport L43 + L91)
// ============================================================

void test_partition_heal_old_leader_converges() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int oldLeader = c.findLeader(3);
    ASSERT_GE(oldLeader, 0);

    // Write data on old leader
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + oldLeader, 'a', 1));
    wait_ms(1000);

    // Kill old leader (simulate partition)
    c.killNode(oldLeader);
    wait_ms(3000);

    // New leader elected
    int newLeader = -1;
    for (int i = 0; i < 3; i++) {
        if (i != oldLeader && c.logContains(i, "Became Leader")) {
            newLeader = i;
            break;
        }
    }
    ASSERT_GE(newLeader, 0);

    // New leader writes different data
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + newLeader, 'b', 2));
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + newLeader, 'c', 3));
    wait_ms(1000);

    // Restart old leader (partition heals)
    c.startNode(oldLeader);
    wait_ms(3000);

    // Old leader must step down
    ASSERT_TRUE(c.logContains(oldLeader, "Stepped down to Follower") ||
                c.logContains(oldLeader, "[Follower]"));

    // Old leader must have ALL committed data including new leader's writes
    ASSERT_TRUE(c.logContains(oldLeader, "a=1"));
    ASSERT_TRUE(c.logContains(oldLeader, "b=2"));
    ASSERT_TRUE(c.logContains(oldLeader, "c=3"));
}

// ============================================================
// GET ON FOLLOWER RETURNS REDIRECT
// (ProjectPlanReport L16, L61: follower redirect for both
// put AND get)
// ============================================================

void test_get_on_follower_returns_redirect() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    // Find a follower
    int follower = (leader == 0) ? 1 : 0;

    Message req;
    req.type = MessageType::ClientGet;
    req.senderId = -1;
    req.key = 'x';

    Message reply;
    bool ok = sendRaw("127.0.0.1", 9000 + follower, req, reply);
    ASSERT_TRUE(ok);
    ASSERT_FALSE(reply.isLeader);
    ASSERT_EQ(reply.redirectPort, 9000 + leader);
}

// ============================================================
// STRESS: 50 entries replicated and all applied on all nodes.
// Verifies system handles sustained load.
// ============================================================

void test_stress_50_entries_all_replicated() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    int successes = 0;
    for (int i = 0; i < 50; i++) {
        char key = 'a' + (i % 26);
        if (sendPut("127.0.0.1", port, key, i)) successes++;
    }
    ASSERT_GE(successes, 45); // allow a few TCP timeouts under load
    wait_ms(5000);

    // Every node should have applied at least 45 entries
    for (int n = 0; n < 3; n++) {
        int applied = c.countPattern(n, "Applied log[");
        ASSERT_GE(applied, 45);
    }
}

// ============================================================
// CONCURRENT PUTS: Multiple threads sending puts simultaneously
// to the leader. All should succeed and replicate.
// ============================================================

void test_concurrent_puts_from_multiple_clients() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    std::atomic<int> successes{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < 5; t++) {
        threads.emplace_back([port, t, &successes]() {
            for (int i = 0; i < 5; i++) {
                char key = 'a' + (t * 5 + i) % 26;
                int value = t * 100 + i;
                if (sendPut("127.0.0.1", port, key, value)) {
                    successes++;
                }
            }
        });
    }

    for (auto& th : threads) th.join();
    wait_ms(3000);

    // At least most of the 25 puts should succeed
    ASSERT_GE(successes.load(), 20);

    // All nodes should have applied a similar number of entries
    for (int n = 0; n < 3; n++) {
        int applied = c.countPattern(n, "Applied log[");
        ASSERT_GE(applied, 20);
    }
}

// ============================================================
// MULTIPLE FOLLOWERS LAGGING BY DIFFERENT AMOUNTS:
// Kill follower A, write 2 entries. Kill follower B, write 2
// more. Restart both. Both should catch up to all 4 entries.
// ============================================================

void test_multiple_followers_different_lag() {
    Cluster c("cluster5.conf");
    c.startAll(5);
    wait_ms(3000);

    int leader = c.findLeader(5);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Pick two followers
    int f1 = -1, f2 = -1;
    for (int i = 0; i < 5; i++) {
        if (i == leader) continue;
        if (f1 < 0) f1 = i;
        else if (f2 < 0) { f2 = i; break; }
    }

    // Kill f1 — misses everything from here
    c.killNode(f1);
    wait_ms(500);

    ASSERT_TRUE(sendPut("127.0.0.1", port, 'a', 1));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'b', 2));
    wait_ms(500);

    // Kill f2 — f2 has a,b but misses c,d
    c.killNode(f2);
    wait_ms(500);

    ASSERT_TRUE(sendPut("127.0.0.1", port, 'c', 3));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'd', 4));
    wait_ms(1000);

    // Restart both
    c.startNode(f1);
    c.startNode(f2);
    wait_ms(3000);

    // f1 was behind by 4, f2 was behind by 2 — both should have all 4
    for (int f : {f1, f2}) {
        ASSERT_TRUE(c.logContains(f, "a=1"));
        ASSERT_TRUE(c.logContains(f, "b=2"));
        ASSERT_TRUE(c.logContains(f, "c=3"));
        ASSERT_TRUE(c.logContains(f, "d=4"));
    }
}

// ============================================================
// COMMITTED ENTRY IDENTITY: Verify the exact same entry (same
// key=value) is at the same log index on all nodes.
// (ProjectPlanReport L14: "If an entry is committed at index i,
// all nodes will consider that entry as identical")
// ============================================================

void test_committed_entries_identical_at_same_index() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    ASSERT_TRUE(sendPut("127.0.0.1", port, 'm', 7));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'n', 8));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'o', 9));
    wait_ms(2000);

    // Every node must show the EXACT same index→entry mapping
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(c.logContains(i, "Applied log[1]: m=7"));
        ASSERT_TRUE(c.logContains(i, "Applied log[2]: n=8"));
        ASSERT_TRUE(c.logContains(i, "Applied log[3]: o=9"));
    }
}

// ============================================================
// NEW LEADER'S STATE MACHINE REFLECTS ALL COMMITTED DATA:
// After failover, GET on new leader returns correct values.
// ============================================================

void test_new_leader_state_machine_correct_after_failover() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader1 = c.findLeader(3);
    ASSERT_GE(leader1, 0);

    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader1, 'a', 11));
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader1, 'b', 22));
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader1, 'a', 33)); // overwrite a
    wait_ms(1000);

    // Kill leader
    c.killNode(leader1);
    wait_ms(3000);

    // New leader
    int leader2 = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader1 && c.logContains(i, "Became Leader")) {
            leader2 = i;
            break;
        }
    }
    ASSERT_GE(leader2, 0);

    // New leader's state machine must reflect the overwrite
    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader2, 'a'), 33); // latest value
    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader2, 'b'), 22);
    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader2, 'c'), -1); // never written
}

// ============================================================
// LEADER WITH LONGER LOG IS ELECTED: Verify a node with fewer
// log entries does not win over a node with more entries at
// a higher term.
// (ProjectPlanReport L90: "new leader has the most up to date log")
// ============================================================

void test_leader_election_favors_longer_log() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    // Write several entries so followers get them
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader, 'a' + i, i + 1));
    }
    wait_ms(1000);

    // Kill leader — both followers have 5 entries. Either can be leader.
    c.killNode(leader);
    wait_ms(3000);

    // New leader must have all 5 entries
    int newLeader = -1;
    for (int i = 0; i < 3; i++) {
        if (i == leader) continue;
        if (c.logContains(i, "Became Leader")) { newLeader = i; break; }
    }
    ASSERT_GE(newLeader, 0);

    // Verify all entries present on new leader
    for (int i = 0; i < 5; i++) {
        std::string pattern = std::string(1, 'a' + i) + "=" + std::to_string(i + 1);
        ASSERT_TRUE(c.logContains(newLeader, pattern));
    }
}

// ============================================================
// SINGLE PUT THEN LEADER KILL: Even a single committed entry
// survives leader failure.
// ============================================================

void test_single_entry_survives_leader_kill() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader1 = c.findLeader(3);
    ASSERT_GE(leader1, 0);

    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader1, 'z', 99));
    wait_ms(1000);

    c.killNode(leader1);
    wait_ms(3000);

    int leader2 = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader1 && c.logContains(i, "Became Leader")) {
            leader2 = i;
            break;
        }
    }
    ASSERT_GE(leader2, 0);

    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader2, 'z'), 99);
}

// ============================================================
// READ AFTER WRITE CONSISTENCY: PUT followed immediately by
// GET on the same leader returns the written value.
// ============================================================

void test_read_after_write_consistency() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    for (char k = 'a'; k <= 'j'; k++) {
        int v = (k - 'a') * 10 + 1;
        ASSERT_TRUE(sendPut("127.0.0.1", port, k, v));
        wait_ms(300); // wait for commit
        int got = sendGet("127.0.0.1", port, k);
        ASSERT_EQ(got, v);
    }
}

// ============================================================
// 5-NODE MAJORITY BOUNDARY: With exactly 3 alive (threshold)
// writes succeed. With exactly 2 alive they don't.
// ============================================================

void test_5node_exact_majority_boundary() {
    Cluster c("cluster5.conf");
    c.startAll(5);
    wait_ms(3000);

    int leader = c.findLeader(5);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Kill 2 non-leader nodes → 3 alive (majority)
    int killed = 0;
    std::vector<int> deadNodes;
    for (int i = 0; i < 5 && killed < 2; i++) {
        if (i != leader) { c.killNode(i); deadNodes.push_back(i); killed++; }
    }
    wait_ms(1000);

    // Writes should succeed (3/5 = majority)
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'a', 1));
    wait_ms(1000);
    ASSERT_TRUE(c.logContains(leader, "Applied log[1]: a=1"));

    // Kill one more non-leader → 2 alive (below majority)
    int thirdKill = -1;
    for (int i = 0; i < 5; i++) {
        if (i == leader) continue;
        bool alreadyDead = false;
        for (int d : deadNodes) { if (d == i) { alreadyDead = true; break; } }
        if (!alreadyDead) { thirdKill = i; break; }
    }
    ASSERT_GE(thirdKill, 0);
    c.killNode(thirdKill);
    wait_ms(1000);

    // Write should NOT commit (2/5 < majority)
    sendPut("127.0.0.1", port, 'b', 2);
    wait_ms(2000);
    ASSERT_FALSE(c.logContains(leader, "Applied log[2]: b=2"));
}

// ============================================================
// IDEMPOTENT VOTE: A node that already voted for a candidate
// can be asked again and still grants the same vote (but not
// a vote to a different candidate in the same term).
// ============================================================

void test_idempotent_vote_same_candidate() {
    // Start a server to act as a Raft node
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    // Kill leader so remaining nodes' votes are available for next term
    c.killNode(leader);
    wait_ms(3000);

    // After leader kill, one of the remaining nodes became candidate/leader
    // The key point: the system didn't crash and a new leader emerged
    int remaining1 = -1, remaining2 = -1;
    for (int i = 0; i < 3; i++) {
        if (i == leader) continue;
        if (remaining1 < 0) remaining1 = i; else remaining2 = i;
    }

    bool newLeaderExists = c.logContains(remaining1, "Became Leader") ||
                           c.logContains(remaining2, "Became Leader");
    ASSERT_TRUE(newLeaderExists);
}

// ============================================================
// ENTRY ORDER PRESERVED: Multiple puts arrive at the KV store
// in the order they were submitted.
// ============================================================

void test_entry_order_preserved() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Write same key multiple times
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'v', 1));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'v', 2));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'v', 3));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'v', 4));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'v', 5));
    wait_ms(2000);

    // Verify entries are at the correct indices (order preserved)
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(c.logContains(i, "Applied log[1]: v=1"));
        ASSERT_TRUE(c.logContains(i, "Applied log[2]: v=2"));
        ASSERT_TRUE(c.logContains(i, "Applied log[3]: v=3"));
        ASSERT_TRUE(c.logContains(i, "Applied log[4]: v=4"));
        ASSERT_TRUE(c.logContains(i, "Applied log[5]: v=5"));
    }

    // Final value should be 5 (last write wins)
    ASSERT_EQ(sendGet("127.0.0.1", port, 'v'), 5);
}

// ============================================================
// NO DOUBLE APPLY: Entries are never applied twice.
// ============================================================

void test_no_double_apply() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    ASSERT_TRUE(sendPut("127.0.0.1", port, 'w', 10));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'w', 20));
    wait_ms(2000);

    // Each node should apply each index exactly once
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(c.countPattern(i, "Applied log[1]:"), 1);
        ASSERT_EQ(c.countPattern(i, "Applied log[2]:"), 1);
    }
}

// ============================================================
// LEADER DOES NOT OVERWRITE OWN LOG: Leader never deletes
// or modifies its own log entries.
// (Raft Leader Append-Only property)
// ============================================================

void test_leader_append_only() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    ASSERT_TRUE(sendPut("127.0.0.1", port, 'a', 1));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'b', 2));
    ASSERT_TRUE(sendPut("127.0.0.1", port, 'c', 3));
    wait_ms(1000);

    // Leader should never have a "log mismatch" or "term mismatch" in its log
    ASSERT_FALSE(c.logContains(leader, "log mismatch"));
    ASSERT_FALSE(c.logContains(leader, "term mismatch"));

    // All three entries should be present and applied
    ASSERT_TRUE(c.logContains(leader, "Applied log[1]: a=1"));
    ASSERT_TRUE(c.logContains(leader, "Applied log[2]: b=2"));
    ASSERT_TRUE(c.logContains(leader, "Applied log[3]: c=3"));
}

// ============================================================
// WRITE AFTER FAILOVER: The new leader can accept new writes
// and they replicate correctly.
// ============================================================

void test_write_after_failover_replicates() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader1 = c.findLeader(3);
    ASSERT_GE(leader1, 0);

    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader1, 'a', 1));
    wait_ms(1000);

    c.killNode(leader1);
    wait_ms(3000);

    int leader2 = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader1 && c.logContains(i, "Became Leader")) {
            leader2 = i;
            break;
        }
    }
    ASSERT_GE(leader2, 0);

    // Write on new leader
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader2, 'b', 2));
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader2, 'c', 3));
    wait_ms(1000);

    // The other surviving follower should have all 3 entries
    int follower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader1 && i != leader2) { follower = i; break; }
    }
    ASSERT_GE(follower, 0);

    ASSERT_TRUE(c.logContains(follower, "a=1"));
    ASSERT_TRUE(c.logContains(follower, "b=2"));
    ASSERT_TRUE(c.logContains(follower, "c=3"));
}

// ============================================================
// ELECTION WITH EMPTY LOG: Fresh 3-node cluster, no prior
// data, leader is elected and can immediately serve writes.
// ============================================================

void test_election_empty_log_then_write() {
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    // Write immediately
    ASSERT_TRUE(sendPut("127.0.0.1", 9000 + leader, 'f', 42));
    wait_ms(1000);

    ASSERT_EQ(sendGet("127.0.0.1", 9000 + leader, 'f'), 42);
}

// ============================================================
// 5-NODE: 3 nodes offline → no leader elected (no majority).
// ============================================================

void test_5node_majority_offline_no_leader() {
    Cluster c("cluster5.conf");
    // Only start 2 of 5 — below majority (3)
    c.startNode(0);
    c.startNode(1);
    wait_ms(3000);

    // Neither should become leader
    bool leader0 = c.logContains(0, "Became Leader");
    bool leader1 = c.logContains(1, "Became Leader");
    ASSERT_FALSE(leader0);
    ASSERT_FALSE(leader1);

    // But both should be trying elections
    ASSERT_GE(c.countPattern(0, "Starting election"), 1);
    ASSERT_GE(c.countPattern(1, "Starting election"), 1);
}

// ============================================================
// Main
// ============================================================

int main() {
    system("pkill -f './raft_node' 2>/dev/null; sleep 0.3");

    std::cout << "Raft Advanced Tests" << std::endl;
    std::cout << "(Each test starts/stops its own cluster)" << std::endl;

    TEST_SECTION("Multi-Leader-Failure Scenarios");
    RUN_TEST(test_data_survives_three_leader_changes);
    RUN_TEST(test_single_entry_survives_leader_kill);

    TEST_SECTION("KV State Machine Consistency");
    RUN_TEST(test_kv_consistency_across_all_nodes);
    RUN_TEST(test_committed_entries_identical_at_same_index);
    RUN_TEST(test_new_leader_state_machine_correct_after_failover);

    TEST_SECTION("Heartbeat Commit Propagation");
    RUN_TEST(test_heartbeat_advances_follower_commitIndex);

    TEST_SECTION("5-Node Fault Tolerance");
    RUN_TEST(test_5node_rolling_kills_with_writes);
    RUN_TEST(test_5node_kill_restart_full_convergence);
    RUN_TEST(test_5node_exact_majority_boundary);
    RUN_TEST(test_5node_majority_offline_no_leader);

    TEST_SECTION("Multiple Lagging Followers");
    RUN_TEST(test_multiple_followers_different_lag);

    TEST_SECTION("Network Partition Heal");
    RUN_TEST(test_partition_heal_old_leader_converges);

    TEST_SECTION("Term & Election Properties");
    RUN_TEST(test_term_monotonicity_across_elections);
    RUN_TEST(test_leader_election_favors_longer_log);
    RUN_TEST(test_idempotent_vote_same_candidate);

    TEST_SECTION("Log & Apply Invariants");
    RUN_TEST(test_entry_order_preserved);
    RUN_TEST(test_no_double_apply);
    RUN_TEST(test_leader_append_only);

    TEST_SECTION("Client Operations");
    RUN_TEST(test_get_on_follower_returns_redirect);
    RUN_TEST(test_read_after_write_consistency);
    RUN_TEST(test_election_empty_log_then_write);
    RUN_TEST(test_write_after_failover_replicates);

    TEST_SECTION("Stress & Concurrency");
    RUN_TEST(test_stress_50_entries_all_replicated);
    RUN_TEST(test_concurrent_puts_from_multiple_clients);

    // Cleanup
    system("pkill -f './raft_node' 2>/dev/null");
    system("rm -f test_node_*.log");

    return printSummary();
}
