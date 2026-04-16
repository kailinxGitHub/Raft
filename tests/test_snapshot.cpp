// test_snapshot.cpp
// Integration tests for:
//   - Client forwarding (follower redirect behavior)
//   - Individual snapshots (log compaction triggered by threshold)
//   - Snapshot persistence (restart from snapshot_<id>.dat)
//   - InstallSnapshot RPC (leader sends snapshot to lagging follower)
//
// Uses the same Cluster harness as test_integration.cpp.
// Requires ./raft_node binary to be built first (make raft_node).

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

// ============================================================
// Test Harness (same pattern as test_integration.cpp)
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
                      std::to_string(nodeId).c_str(),
                      confFile.c_str(),
                      ft.c_str(), nullptr);
            } else {
                execl(BINARY.c_str(), BINARY.c_str(),
                      std::to_string(nodeId).c_str(),
                      confFile.c_str(), nullptr);
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

    bool isRunning(int nodeId) const {
        for (const auto& n : nodes) {
            if (n.nodeId == nodeId) return n.pid > 0;
        }
        return false;
    }

    std::string readLog(int nodeId) const {
        std::string logFile = "test_node_" + std::to_string(nodeId) + ".log";
        std::ifstream f(logFile);
        std::ostringstream oss;
        oss << f.rdbuf();
        return oss.str();
    }

    bool logContains(int nodeId, const std::string& pattern) const {
        return readLog(nodeId).find(pattern) != std::string::npos;
    }

    int countPattern(int nodeId, const std::string& pattern) const {
        std::string lg = readLog(nodeId);
        int count = 0;
        size_t pos = 0;
        while ((pos = lg.find(pattern, pos)) != std::string::npos) {
            count++;
            pos += pattern.size();
        }
        return count;
    }

    int findLeader(int nodeCount) const {
        for (int i = 0; i < nodeCount; i++) {
            if (logContains(i, "Became Leader")) return i;
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

// Send a ClientPut and return the full raw reply (does NOT require success)
static Message sendPutRaw(const std::string& host, int port, char key, int value) {
    Message req;
    req.type     = MessageType::ClientPut;
    req.senderId = -1;
    req.key      = key;
    req.value    = value;
    Message reply;
    sendRPC(host, port, req, reply);
    return reply;
}

// Send a ClientPut to a leader; returns true on success
static bool sendPut(const std::string& host, int port, char key, int value) {
    Message reply = sendPutRaw(host, port, key, value);
    return reply.success && reply.isLeader;
}

// Send a ClientGet with retry for read-safety confirmation
static int sendGet(const std::string& host, int port, char key) {
    Message req;
    req.type     = MessageType::ClientGet;
    req.senderId = -1;
    req.key      = key;
    for (int attempt = 0; attempt < 5; attempt++) {
        Message reply;
        if (!sendRPC(host, port, req, reply)) return -999;
        if (!reply.isLeader) return -998;
        if (reply.success) return reply.value;
        wait_ms(150);
    }
    return -997;
}

// Remove all snapshot and persistent state files (call before tests that need a clean state)
static void cleanupSnapshotFiles(int nodeCount = 5) {
    for (int i = 0; i < nodeCount; i++) {
        ::unlink(("snapshot_"   + std::to_string(i) + ".dat").c_str());
        ::unlink(("raft_state_" + std::to_string(i) + ".dat").c_str());
    }
}

// Check whether a snapshot file exists and has a valid lastIncludedIndex
static int readSnapshotIndex(int nodeId) {
    std::string path = "snapshot_" + std::to_string(nodeId) + ".dat";
    std::ifstream f(path);
    if (!f.is_open()) return -1;
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("lastIncludedIndex=", 0) == 0) {
            return std::stoi(line.substr(18));
        }
    }
    return 0;
}

static int readSnapshotKV(int nodeId, char key) {
    std::string path = "snapshot_" + std::to_string(nodeId) + ".dat";
    std::ifstream f(path);
    if (!f.is_open()) return -1;
    std::string prefix = "kv:";
    prefix += key;
    prefix += "=";
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind(prefix, 0) == 0) {
            return std::stoi(line.substr(prefix.size()));
        }
    }
    return -1;
}

// ============================================================
// CLIENT FORWARDING TESTS
// ============================================================

// "If a follower node receives a message from a client,
//  it will respond with the current leader's network address."
//  — ProjectPlanReport.txt, Client Forwarding goal
void test_client_forwarding_follower_redirect() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    // Find a follower (not the leader)
    int follower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader) { follower = i; break; }
    }
    ASSERT_GE(follower, 0);

    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& fn = cfg.getNode(follower);

    // Send PUT directly to follower
    Message reply = sendPutRaw(fn.hostname, fn.port, 'X', 99);

    // Follower must not claim to be leader
    ASSERT_FALSE(reply.isLeader);
    ASSERT_FALSE(reply.success);

    // Follower must provide a redirect to the actual leader
    ASSERT_FALSE(reply.redirectHost.empty());
    ASSERT_GT(reply.redirectPort, 0);

    // Verify redirect points to a valid cluster node
    bool validRedirect = false;
    for (const auto& node : cfg.nodes) {
        if (node.hostname == reply.redirectHost && node.port == reply.redirectPort) {
            validRedirect = true;
            break;
        }
    }
    ASSERT_TRUE(validRedirect);
}

// When no leader is known yet (very early), a follower should still
// handle the redirect gracefully (isLeader=false, success=false).
void test_client_forwarding_no_known_leader() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    // Start only one node — it can't elect itself as leader (needs majority of 3=2)
    c.startNode(0);
    wait_ms(500); // Not enough time for an election to succeed

    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& n0 = cfg.getNode(0);

    Message reply = sendPutRaw(n0.hostname, n0.port, 'Y', 1);
    // Node is alone — either no reply (RPC fails) or isLeader=false
    // We just check it doesn't claim to be leader successfully
    if (reply.type == MessageType::ClientResponse) {
        ASSERT_FALSE(reply.success && reply.isLeader);
    }
    // Pass: either couldn't connect (acceptable) or correctly rejected
}

// A client that follows the redirect chain should successfully store a value.
void test_client_forwarding_auto_retry_reaches_leader() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    int follower = (leader + 1) % 3;
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& fn = cfg.getNode(follower);

    // Try PUT at follower — it redirects
    Message r1 = sendPutRaw(fn.hostname, fn.port, 'F', 77);
    ASSERT_FALSE(r1.isLeader);
    ASSERT_FALSE(r1.redirectHost.empty());

    // Follow the redirect
    Message r2 = sendPutRaw(r1.redirectHost, r1.redirectPort, 'F', 77);
    ASSERT_TRUE(r2.isLeader);
    ASSERT_TRUE(r2.success);

    // Wait for the PUT to be committed and applied (replication is async)
    wait_ms(600);

    // Confirm the value is readable from the leader
    int val = sendGet(r1.redirectHost, r1.redirectPort, 'F');
    ASSERT_EQ(val, 77);
}

// ============================================================
// INDIVIDUAL SNAPSHOT TESTS
// ============================================================

// "Write system state to a snapshot and discard log up to that point"
// Verify: after SNAPSHOT_LOG_THRESHOLD+N puts, each node's log is compacted.
void test_snapshot_trigger_and_compaction() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Send 60 entries (> SNAPSHOT_LOG_THRESHOLD=50) using keys a-z circling twice
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        bool ok = sendPut(ln.hostname, ln.port, key, i);
        ASSERT_TRUE(ok);
    }

    // Give nodes time to apply and compact
    wait_ms(2000);

    // At least one node should have created a snapshot file
    bool anySnapshot = false;
    for (int i = 0; i < 3; i++) {
        if (readSnapshotIndex(i) > 0) { anySnapshot = true; break; }
    }
    ASSERT_TRUE(anySnapshot);

    // Log files should contain "Snapshot taken" events
    bool anyCompactionLog = false;
    for (int i = 0; i < 3; i++) {
        if (c.logContains(i, "Snapshot taken") ||
            c.logContains(i, "snapshot at index")) {
            anyCompactionLog = true; break;
        }
    }
    ASSERT_TRUE(anyCompactionLog);
}

// After snapshot, data is still correctly accessible.
void test_snapshot_correctness_after_compaction() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Write a known value, then trigger snapshot
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'K', 42));

    // Fill up to trigger compaction
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        sendPut(ln.hostname, ln.port, key, i);
    }
    wait_ms(2000);

    // Overwrite K with a new value after potential snapshot
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'K', 99));
    wait_ms(500);

    // GET should return the latest value
    int val = sendGet(ln.hostname, ln.port, 'K');
    ASSERT_EQ(val, 99);
}

// "Each server takes snapshots independently"
// Verify snapshot files are written for each node independently.
void test_snapshot_each_node_independently() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Put 60 entries
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        sendPut(ln.hostname, ln.port, key, i);
    }
    wait_ms(3000);

    // All three nodes should eventually have a snapshot
    int snapshotCount = 0;
    for (int i = 0; i < 3; i++) {
        if (readSnapshotIndex(i) > 0) snapshotCount++;
    }
    // At least 2 out of 3 nodes should have taken a snapshot (majority committed)
    ASSERT_GE(snapshotCount, 2);
}

// ============================================================
// SNAPSHOT PERSISTENCE TESTS
// ============================================================

// "kill node and then restart same binary with same data directory;
//  node rejoins and catches up with no difference"
// — ProjectPlanReport.txt, Persistence evaluation criteria
void test_snapshot_persistence_restart() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Put known key-value pairs
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'P', 11));
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'Q', 22));

    // Trigger snapshot on all nodes by putting many entries
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        sendPut(ln.hostname, ln.port, key, i * 2);
    }
    wait_ms(2500);

    // Find a follower, kill it, then restart — it should load from snapshot
    int follower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader) { follower = i; break; }
    }
    ASSERT_GE(follower, 0);

    // Verify snapshot file exists before kill
    int snapshotIdx = readSnapshotIndex(follower);
    ASSERT_GT(snapshotIdx, 0);

    c.killNode(follower);
    wait_ms(500);

    // Restart the follower (it will load snapshot on startup)
    c.startNode(follower);
    wait_ms(2000);

    // Log should say "Restored from snapshot"
    ASSERT_TRUE(c.logContains(follower, "Restored from snapshot"));
}

// Snapshot file contains correct KV data after compaction.
void test_snapshot_file_contains_correct_kv() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Write a specific known value
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'Z', 55));

    // Fill up to trigger snapshot
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        sendPut(ln.hostname, ln.port, key, i + 100);
    }
    wait_ms(2500);

    // Check that the snapshot file for the leader contains key 'Z'=55 OR
    // the final value of 'z' from the loop (which overwrites 'Z' is 'z' not 'Z').
    // 'Z' was written before the loop and was not overwritten, so should be 55.
    // (Note: 'Z' = 90 decimal, the loop uses 'a' to 'z' = 97..122, no overlap with 'Z')
    bool foundZ = false;
    for (int i = 0; i < 3; i++) {
        int v = readSnapshotKV(i, 'Z');
        if (v == 55) { foundZ = true; break; }
    }
    ASSERT_TRUE(foundZ);
}

// ============================================================
// INSTALL SNAPSHOT TESTS (Stretch Goal)
// ============================================================

// "Followers may receive snapshots from leader when they are too far behind"
// — ProjectPlanReport.txt, Snapshot capabilities stretch goal
//
// Scenario: kill follower B → put 60+ entries (triggers snapshot on A+C)
//           → restart B → B receives InstallSnapshot from leader.
void test_install_snapshot_lagging_follower() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Find a follower to kill
    int deadFollower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader) { deadFollower = i; break; }
    }
    ASSERT_GE(deadFollower, 0);

    // Kill the follower before any entries
    c.killNode(deadFollower);
    wait_ms(300);

    // Put 60 entries — committed by leader + remaining follower (majority=2)
    // This will trigger snapshot on both active nodes
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        bool ok = sendPut(ln.hostname, ln.port, key, i + 1);
        ASSERT_TRUE(ok);
    }
    // Write a specific sentinel we'll verify after catch-up
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'S', 777));
    wait_ms(2500);

    // Leader should have a snapshot now
    ASSERT_GT(readSnapshotIndex(leader), 0);

    // Restart the dead follower (no snapshot file for it — it was offline)
    // It should receive InstallSnapshot from the leader
    c.startNode(deadFollower);
    wait_ms(3000);

    // Follower should log "Installed snapshot"
    ASSERT_TRUE(c.logContains(deadFollower, "Installed snapshot") ||
                c.logContains(deadFollower, "Restored from snapshot"));

    // After catching up, GET from the follower (via leader redirect)
    // should return correct values
    const auto& dn = cfg.getNode(deadFollower);
    Message r = sendPutRaw(dn.hostname, dn.port, 'T', 0); // just to find leader
    std::string targetHost = ln.hostname;
    int targetPort = ln.port;
    if (!r.isLeader && !r.redirectHost.empty()) {
        targetHost = r.redirectHost;
        targetPort = r.redirectPort;
    }
    int val = sendGet(targetHost, targetPort, 'S');
    ASSERT_EQ(val, 777);
}

// After InstallSnapshot, newly appended entries replicate correctly.
void test_install_snapshot_then_new_entries() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Kill follower
    int deadFollower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader) { deadFollower = i; break; }
    }

    c.killNode(deadFollower);
    wait_ms(300);

    // Trigger snapshot on remaining nodes
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        sendPut(ln.hostname, ln.port, key, i);
    }
    wait_ms(2500);

    // Restart follower → gets InstallSnapshot
    c.startNode(deadFollower);
    wait_ms(3000);

    // Now write NEW entries after the snapshot — these should replicate normally
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'N', 88));
    wait_ms(1000);

    // Read back the new entry
    int val = sendGet(ln.hostname, ln.port, 'N');
    ASSERT_EQ(val, 88);
}

// Two lagging followers both catch up via InstallSnapshot.
void test_install_snapshot_two_lagging_followers() {
    cleanupSnapshotFiles();
    Cluster c("cluster5.conf");
    c.startAll(5);
    wait_ms(3000);

    int leader = c.findLeader(5);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster5.conf");
    const auto& ln = cfg.getNode(leader);

    // Kill two followers
    int dead1 = -1, dead2 = -1;
    for (int i = 0; i < 5; i++) {
        if (i == leader) continue;
        if (dead1 < 0) { dead1 = i; continue; }
        if (dead2 < 0) { dead2 = i; break; }
    }
    ASSERT_GE(dead1, 0);
    ASSERT_GE(dead2, 0);

    c.killNode(dead1);
    c.killNode(dead2);
    wait_ms(300);

    // 3 remaining nodes = majority of 5 → can still commit
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        bool ok = sendPut(ln.hostname, ln.port, key, i + 10);
        ASSERT_TRUE(ok);
    }
    ASSERT_TRUE(sendPut(ln.hostname, ln.port, 'M', 123));
    wait_ms(2500);

    // Restart both lagging followers
    c.startNode(dead1);
    c.startNode(dead2);
    wait_ms(4000);

    // Both should have received snapshots
    ASSERT_TRUE(c.logContains(dead1, "Installed snapshot") ||
                c.logContains(dead1, "Restored from snapshot"));
    ASSERT_TRUE(c.logContains(dead2, "Installed snapshot") ||
                c.logContains(dead2, "Restored from snapshot"));

    // System should still serve reads correctly
    int val = sendGet(ln.hostname, ln.port, 'M');
    ASSERT_EQ(val, 123);
}

// ============================================================
// COMBINED: SNAPSHOT + ELECTION STABILITY
// ============================================================

// After log compaction, new leader elections still work correctly.
void test_snapshot_does_not_break_election() {
    cleanupSnapshotFiles();
    Cluster c("cluster.conf");
    c.startAll(3);
    wait_ms(2500);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    ClusterConfig cfg = ClusterConfig::load("cluster.conf");
    const auto& ln = cfg.getNode(leader);

    // Trigger snapshot
    for (int i = 0; i < 60; i++) {
        char key = 'a' + (i % 26);
        sendPut(ln.hostname, ln.port, key, i);
    }
    wait_ms(2000);

    // Kill the leader — a new election should occur
    c.killNode(leader);
    wait_ms(3000);

    // A new leader should be elected among the remaining two nodes
    int newLeader = -1;
    for (int i = 0; i < 3; i++) {
        if (i == leader) continue;
        if (c.countPattern(i, "Became Leader") >= 1) { newLeader = i; break; }
    }
    ASSERT_GE(newLeader, 0);

    // New leader should still serve writes
    const auto& nln = cfg.getNode(newLeader);
    bool ok = sendPut(nln.hostname, nln.port, 'E', 5);
    ASSERT_TRUE(ok);
}

// ============================================================
// main
// ============================================================

int main() {
    std::cout << "=== Snapshot & Client Forwarding Tests ===" << std::endl;
    std::cout << "NOTE: These tests require ./raft_node to be built first.\n" << std::endl;

    TEST_SECTION("Client Forwarding");
    RUN_TEST(test_client_forwarding_follower_redirect);
    RUN_TEST(test_client_forwarding_no_known_leader);
    RUN_TEST(test_client_forwarding_auto_retry_reaches_leader);

    TEST_SECTION("Individual Snapshots");
    RUN_TEST(test_snapshot_trigger_and_compaction);
    RUN_TEST(test_snapshot_correctness_after_compaction);
    RUN_TEST(test_snapshot_each_node_independently);
    RUN_TEST(test_snapshot_file_contains_correct_kv);

    TEST_SECTION("Snapshot Persistence");
    RUN_TEST(test_snapshot_persistence_restart);

    TEST_SECTION("InstallSnapshot (Stretch Goal)");
    RUN_TEST(test_install_snapshot_lagging_follower);
    RUN_TEST(test_install_snapshot_then_new_entries);
    RUN_TEST(test_install_snapshot_two_lagging_followers);

    TEST_SECTION("Election Stability After Snapshot");
    RUN_TEST(test_snapshot_does_not_break_election);

    // Cleanup snapshot files left by tests
    cleanupSnapshotFiles();

    return printSummary();
}
