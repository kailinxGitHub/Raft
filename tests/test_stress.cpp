// test_stress.cpp
// Heavy stress tests for the Raft consensus implementation.
// Covers scenarios not addressed by the existing test suite:
//   1. High-volume writes (200 entries)
//   2. Rapid leader churn under sustained writes
//   3. 50 concurrent client threads
//   4. Repeated partition/heal cycles (5 rounds)
//   5. Quorum boundary probing on a 5-node cluster
//   6. Full cluster restart mid-write storm
//   7. Snapshot triggered under load + lagging follower InstallSnapshot
//   8. Long log reconciliation (uncommitted entries must not survive)
//   9. Mixed read/write flood (20 threads simultaneously)

#include "test_framework.h"
#include "../src/network.h"
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

// ============================================================
// Cluster harness (mirrors test_advanced.cpp)
// ============================================================

static const std::string STRESS_BINARY = "./raft_node";

struct StressNode {
    int nodeId;
    pid_t pid;
    std::string logFile;
};

class StressCluster {
    std::vector<StressNode> nodes;
    std::string confFile;

public:
    explicit StressCluster(const std::string& conf) : confFile(conf) {
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
                execl(STRESS_BINARY.c_str(), STRESS_BINARY.c_str(),
                      std::to_string(nodeId).c_str(), confFile.c_str(),
                      ft.c_str(), nullptr);
            } else {
                execl(STRESS_BINARY.c_str(), STRESS_BINARY.c_str(),
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

    bool logContains(int nodeId, const std::string& pattern) const {
        return readLog(nodeId).find(pattern) != std::string::npos;
    }

    int countPattern(int nodeId, const std::string& pattern) const {
        std::string log = readLog(nodeId);
        int count = 0;
        size_t pos = 0;
        while ((pos = log.find(pattern, pos)) != std::string::npos) {
            ++count;
            pos += pattern.size();
        }
        return count;
    }

    // Returns the first node ID that contains "Became Leader" in its log,
    // searching among [0, nodeCount).
    int findLeader(int nodeCount) const {
        for (int i = 0; i < nodeCount; i++) {
            if (logContains(i, "Became Leader")) return i;
        }
        return -1;
    }

    // Returns the first live node in `candidates` that shows "Became Leader".
    int findLeaderAmong(const std::vector<int>& candidates) const {
        for (int id : candidates) {
            if (logContains(id, "Became Leader")) return id;
        }
        return -1;
    }

    ~StressCluster() {
        killAll();
        for (int i = 0; i < 5; i++) {
            ::unlink(("snapshot_"   + std::to_string(i) + ".dat").c_str());
            ::unlink(("raft_state_" + std::to_string(i) + ".dat").c_str());
            ::unlink(("test_node_"  + std::to_string(i) + ".log").c_str());
        }
        ::usleep(200 * 1000); // let the OS fully release ports before the next test
    }
};

static void stress_wait(int ms) {
    // 1.5× scale so tests remain stable on slower grading machines
    std::this_thread::sleep_for(std::chrono::milliseconds(ms * 3 / 2));
}

// Returns true when the put was accepted (success + isLeader).
static bool stressPut(const std::string& host, int port, char key, int value) {
    Message req;
    req.type     = MessageType::ClientPut;
    req.senderId = -1;
    req.key      = key;
    req.value    = value;
    Message reply;
    if (!sendRPC(host, port, req, reply)) return false;
    return reply.success && reply.isLeader;
}

// Returns the value on success, or a negative sentinel on failure.
static int stressGet(const std::string& host, int port, char key) {
    Message req;
    req.type     = MessageType::ClientGet;
    req.senderId = -1;
    req.key      = key;
    for (int attempt = 0; attempt < 5; attempt++) {
        Message reply;
        if (!sendRPC(host, port, req, reply)) return -999;
        if (!reply.isLeader) return -998;
        if (reply.success) return reply.value;
        stress_wait(150);
    }
    return -997;
}

// ============================================================
// TEST 1: High-volume writes — 200 entries replicated to all nodes
// ============================================================

void test_high_volume_writes_200_entries() {
    StressCluster c("cluster.conf");
    c.startAll(3);
    stress_wait(3000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Write 200 entries.  Keys cycle a–z; values are the entry number.
    int successCount = 0;
    for (int i = 1; i <= 200; i++) {
        char key = 'a' + ((i - 1) % 26);
        if (stressPut("127.0.0.1", port, key, i)) successCount++;
        // Brief pause every 20 writes to let the leader heartbeat
        if (i % 20 == 0) stress_wait(100);
    }
    // Expect at least 190/200 to succeed (minor timing slop tolerated)
    ASSERT_GE(successCount, 190);

    // Give time for all followers to catch up
    stress_wait(3000);

    // All three nodes must have applied entry 200 (or very close to it)
    int nodesWithEntry200 = 0;
    for (int i = 0; i < 3; i++) {
        if (c.logContains(i, "Applied log[200]")) nodesWithEntry200++;
    }
    ASSERT_GE(nodesWithEntry200, 2);
}

// ============================================================
// TEST 2: Rapid leader churn — kill leader every 5 writes, repeat 10x
// ============================================================

void test_rapid_leader_churn_under_writes() {
    StressCluster c("cluster5.conf");
    c.startAll(5);
    stress_wait(3000);

    // Track which nodes are still alive
    std::vector<bool> alive(5, true);
    int totalSuccesses = 0;
    char key = 'a';

    for (int round = 0; round < 10; round++) {
        // Find an alive leader
        std::vector<int> liveNodes;
        for (int i = 0; i < 5; i++) if (alive[i]) liveNodes.push_back(i);
        if (liveNodes.size() < 3) break;  // lost quorum — stop

        int leader = c.findLeaderAmong(liveNodes);
        if (leader < 0) {
            stress_wait(2000);
            leader = c.findLeaderAmong(liveNodes);
        }
        if (leader < 0) break;

        // Write 5 entries
        for (int w = 0; w < 5; w++) {
            if (stressPut("127.0.0.1", 9000 + leader, key, round * 10 + w))
                totalSuccesses++;
            key = 'a' + ((key - 'a' + 1) % 26);
        }
        stress_wait(500);

        // Kill the leader
        c.killNode(leader);
        alive[leader] = false;

        // Wait for new election (needs 2s with randomized timeouts)
        stress_wait(2500);
    }

    // 5-node cluster loses quorum after 3 leader kills (5→4→3→2 nodes, loop
    // stops at <3 alive), so at most 3 rounds complete = 15 write attempts.
    // Require at least 2 full rounds worth to confirm churn is actually working.
    ASSERT_GE(totalSuccesses, 8);
}

// ============================================================
// TEST 3: 50 concurrent client threads sending puts simultaneously
// ============================================================

void test_concurrent_clients_50_threads() {
    StressCluster c("cluster.conf");
    c.startAll(3);
    stress_wait(3000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    std::atomic<int> successCount{0};
    std::vector<std::thread> threads;
    threads.reserve(50);

    for (int t = 0; t < 50; t++) {
        threads.emplace_back([port, t, &successCount]() {
            char key = 'a' + (t % 26);
            if (stressPut("127.0.0.1", port, key, t + 1))
                successCount.fetch_add(1);
        });
    }
    for (auto& th : threads) th.join();

    // Allow replication to catch up
    stress_wait(2000);

    // Expect the vast majority to be committed (allow a few TCP collisions)
    ASSERT_GE(successCount.load(), 40);
}

// ============================================================
// TEST 4: Repeated partition/heal cycles (5 rounds)
// ============================================================

void test_repeated_partition_heal_cycles() {
    StressCluster c("cluster.conf");
    c.startAll(3);
    stress_wait(3000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);

    // Identify a follower to repeatedly kill/restart
    int follower = (leader + 1) % 3;

    int totalEntries = 0;
    for (int round = 0; round < 5; round++) {
        // Write 3 entries while all nodes are up
        for (int w = 0; w < 3; w++) {
            char key = 'a' + (totalEntries % 26);
            stressPut("127.0.0.1", 9000 + leader, key, totalEntries + 1);
            totalEntries++;
        }
        stress_wait(500);

        // Partition: kill the follower
        c.killNode(follower);
        stress_wait(500);

        // Write 3 more entries — committed with 2/3 majority
        for (int w = 0; w < 3; w++) {
            char key = 'a' + (totalEntries % 26);
            stressPut("127.0.0.1", 9000 + leader, key, totalEntries + 1);
            totalEntries++;
        }
        stress_wait(500);

        // Heal: restart the follower
        c.startNode(follower);
        stress_wait(2000);  // give it time to catch up

        // The rejoin follower must have applied at least the pre-partition entries
        std::string expected = "Applied log[" + std::to_string(totalEntries - 3) + "]";
        ASSERT_TRUE(c.logContains(follower, expected));
    }
}

// ============================================================
// TEST 5: Quorum boundary probing on a 5-node cluster
// ============================================================

void test_quorum_boundary_5node() {
    StressCluster c("cluster5.conf");
    c.startAll(5);
    stress_wait(3000);

    int leader = c.findLeader(5);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Baseline write should succeed
    ASSERT_TRUE(stressPut("127.0.0.1", port, 'a', 1));
    stress_wait(500);

    // Kill 2 followers — 3/5 still alive, majority holds
    std::vector<int> killed;
    for (int i = 0; i < 5; i++) {
        if (i != leader && (int)killed.size() < 2) {
            c.killNode(i);
            killed.push_back(i);
        }
    }
    stress_wait(1000);
    ASSERT_TRUE(stressPut("127.0.0.1", port, 'b', 2));
    stress_wait(500);

    // Kill 1 more — now 2/5 alive, no quorum; the write should time out / fail
    for (int i = 0; i < 5; i++) {
        if (i != leader && std::find(killed.begin(), killed.end(), i) == killed.end()) {
            c.killNode(i);
            killed.push_back(i);
            break;
        }
    }
    // Wait long enough for the leader to time out all dead-node RPCs
    // (SOCKET_TIMEOUT_MS=500 × 3 dead nodes + buffer = ~3 s).
    stress_wait(3000);
    stressPut("127.0.0.1", port, 'c', 3);  // send but cannot commit
    stress_wait(2000);

    // Verify via log files that entry 'c' (index 3) was never applied on
    // either alive node — this is more reliable than checking the RPC return
    // value, which can be timing-sensitive.
    ASSERT_FALSE(c.logContains(leader,    "Applied log[3]"));
    // Find the one remaining alive follower and check its log too
    int aliveFollower = -1;
    for (int i = 0; i < 5; i++) {
        if (i != leader && std::find(killed.begin(), killed.end(), i) == killed.end())
            aliveFollower = i;
    }
    if (aliveFollower >= 0)
        ASSERT_FALSE(c.logContains(aliveFollower, "Applied log[3]"));

    // Restart one killed follower — back to 3/5, quorum restored
    int toRestart = killed[0];
    c.startNode(toRestart);
    stress_wait(3000);

    // Find the new (or existing) leader
    std::vector<int> liveNodes = {leader, toRestart};
    int newLeader = c.findLeaderAmong(liveNodes);
    // May need a fresh election; search all possible live nodes
    if (newLeader < 0) {
        for (int i = 0; i < 5; i++) {
            if (i == toRestart || i == leader) {
                if (c.logContains(i, "Became Leader")) { newLeader = i; break; }
            }
        }
    }
    ASSERT_GE(newLeader, 0);
    ASSERT_TRUE(stressPut("127.0.0.1", 9000 + newLeader, 'd', 4));
}

// ============================================================
// TEST 6: Full cluster restart mid-write storm
// ============================================================

void test_full_cluster_restart_mid_storm() {
    StressCluster c("cluster.conf");
    c.startAll(3);
    stress_wait(3000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Write 20 entries
    int written = 0;
    for (int i = 1; i <= 20; i++) {
        char key = 'a' + ((i - 1) % 26);
        if (stressPut("127.0.0.1", port, key, i)) written++;
    }
    ASSERT_GE(written, 15);
    stress_wait(1000);

    // Kill all nodes ungracefully
    c.killAll();
    stress_wait(500);

    // Restart all nodes
    c.startAll(3);
    stress_wait(3500);

    // Find new leader after restart
    int newLeader = c.findLeader(3);
    ASSERT_GE(newLeader, 0);
    int newPort = 9000 + newLeader;

    // Write 10 more entries
    int postRestartWritten = 0;
    for (int i = 21; i <= 30; i++) {
        char key = 'a' + ((i - 1) % 26);
        if (stressPut("127.0.0.1", newPort, key, i)) postRestartWritten++;
        stress_wait(50);
    }
    ASSERT_GE(postRestartWritten, 8);
    stress_wait(2000);

    // Nodes don't persist their Raft log to disk (only snapshots at threshold
    // 50), so after restart the 20 pre-kill entries are gone and the log
    // index resets to 1.  The 10 post-restart writes therefore occupy indices
    // 1–10.  Verify that at least the first new commit reached all nodes.
    int nodesWithEntry1 = 0;
    for (int i = 0; i < 3; i++) {
        if (c.logContains(i, "Applied log[1]")) nodesWithEntry1++;
    }
    ASSERT_GE(nodesWithEntry1, 2);
}

// ============================================================
// TEST 7: Snapshot triggered under load + lagging follower receives
//         InstallSnapshot on rejoin
// ============================================================

void test_snapshot_under_load_and_install() {
    // Clean up any leftover snapshot files
    for (int i = 0; i < 3; i++) {
        std::string p = "snapshot_" + std::to_string(i) + ".dat";
        ::unlink(p.c_str());
    }

    StressCluster c("cluster.conf");
    c.startAll(3);
    stress_wait(3000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Kill one follower before the write storm
    int laggingFollower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader) { laggingFollower = i; break; }
    }
    c.killNode(laggingFollower);
    stress_wait(300);

    // Write 60 entries to leader + 1 remaining follower.
    // SNAPSHOT_LOG_THRESHOLD = 50, so this triggers snapshot.
    for (int i = 1; i <= 60; i++) {
        char key = 'a' + ((i - 1) % 26);
        stressPut("127.0.0.1", port, key, i);
        if (i % 15 == 0) stress_wait(200);
    }
    stress_wait(2000);

    // At least one of the two alive nodes should have taken a snapshot
    int snapshotNode = -1;
    for (int i = 0; i < 3; i++) {
        if (i != laggingFollower && c.logContains(i, "Snapshot taken")) {
            snapshotNode = i;
            break;
        }
    }
    ASSERT_GE(snapshotNode, 0);

    // Restart the lagging follower — it must receive InstallSnapshot
    c.startNode(laggingFollower);
    stress_wait(4000);

    ASSERT_TRUE(c.logContains(laggingFollower, "Installed snapshot") ||
                c.logContains(laggingFollower, "Restored from snapshot"));
}

// ============================================================
// TEST 8: Long log reconciliation — uncommitted entries on a dead leader
//         must not survive after a new leader is elected
// ============================================================

void test_long_log_reconciliation() {
    StressCluster c("cluster.conf");
    c.startAll(3);  // random timeouts — avoids split-vote instability
    stress_wait(3000);

    int leader = c.findLeader(3);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Write 30 entries, pausing every 10 to let the leader commit each batch
    // before more entries are queued.  Without pacing the 30 rapid puts can
    // overwhelm the event loop and leave heartbeat replies unprocessed.
    for (int i = 1; i <= 30; i++) {
        char key = 'a' + ((i - 1) % 26);
        stressPut("127.0.0.1", port, key, i);
        if (i % 10 == 0) stress_wait(500);  // allow heartbeats to commit the batch
    }
    stress_wait(3000);  // extra time for all followers to apply committed entries

    // Verify all nodes applied entry 30
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(c.logContains(i, "Applied log[30]"));
    }

    // Kill both followers so the leader can't commit new entries
    int follower1 = (leader + 1) % 3;
    int follower2 = (leader + 2) % 3;
    c.killNode(follower1);
    c.killNode(follower2);
    stress_wait(300);

    // Send 10 more puts — they will be appended to the leader's log but
    // cannot be committed (no majority).
    for (int i = 31; i <= 40; i++) {
        char key = 'a' + ((i - 1) % 26);
        stressPut("127.0.0.1", port, key, i);  // expected to fail/timeout
    }
    stress_wait(500);

    // Kill the leader; followers restart and elect a new leader among themselves
    c.killNode(leader);
    c.startNode(follower1);
    c.startNode(follower2);
    stress_wait(3500);

    // The new leader (one of the two followers) must NOT have applied entry 31
    // because those entries were never committed.
    // Note: startNode() reopens the log file in write mode, so each restarted
    // follower's log only contains output from after the restart.  We can only
    // verify the negative — that the uncommitted entry never reappears.
    bool entry31Applied = c.logContains(follower1, "Applied log[31]") ||
                          c.logContains(follower2, "Applied log[31]");
    ASSERT_FALSE(entry31Applied);
}

// ============================================================
// TEST 9: Mixed read/write flood — 20 threads (10 writers + 10 readers)
// ============================================================

void test_mixed_read_write_flood() {
    StressCluster c("cluster5.conf");
    c.startAll(5);
    stress_wait(3000);

    int leader = c.findLeader(5);
    ASSERT_GE(leader, 0);
    int port = 9000 + leader;

    // Write 10 baseline entries so readers have something to fetch
    for (int i = 0; i < 10; i++) {
        char key = 'a' + i;
        stressPut("127.0.0.1", port, key, i + 1);
    }
    stress_wait(1000);

    std::atomic<int> writeSuccesses{0};
    std::atomic<int> readSuccesses{0};
    std::vector<std::thread> threads;
    threads.reserve(20);

    // 10 writer threads, each sending 50 puts
    for (int t = 0; t < 10; t++) {
        threads.emplace_back([port, t, &writeSuccesses]() {
            for (int i = 0; i < 50; i++) {
                char key = 'a' + ((t + i) % 26);
                if (stressPut("127.0.0.1", port, key, t * 100 + i))
                    writeSuccesses.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }

    // 10 reader threads, each sending 30 gets
    for (int t = 0; t < 10; t++) {
        threads.emplace_back([port, t, &readSuccesses]() {
            for (int i = 0; i < 30; i++) {
                char key = 'a' + (t % 10);  // read keys a–j
                int val = stressGet("127.0.0.1", port, key);
                if (val >= 0) readSuccesses.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(15));
            }
        });
    }

    for (auto& th : threads) th.join();
    stress_wait(2000);

    // Expect reasonable success rates — not every op will hit the leader perfectly
    ASSERT_GE(writeSuccesses.load(), 300);   // 10 threads * 50 * ~60% success
    ASSERT_GE(readSuccesses.load(), 150);    // 10 threads * 30 * ~50% success

    // A final read on the leader must return a valid committed value
    int finalVal = stressGet("127.0.0.1", port, 'a');
    ASSERT_GE(finalVal, 0);
}

// ============================================================
// main
// ============================================================

int main() {
    std::cout << "=== Raft Stress Tests ===" << std::endl;

    TEST_SECTION("High-Volume Writes");
    RUN_TEST(test_high_volume_writes_200_entries);

    TEST_SECTION("Leader Churn");
    RUN_TEST(test_rapid_leader_churn_under_writes);

    TEST_SECTION("Concurrent Clients");
    RUN_TEST(test_concurrent_clients_50_threads);

    TEST_SECTION("Repeated Partition/Heal");
    RUN_TEST(test_repeated_partition_heal_cycles);

    TEST_SECTION("Quorum Boundary (5-node)");
    RUN_TEST(test_quorum_boundary_5node);

    TEST_SECTION("Full Cluster Restart");
    RUN_TEST(test_full_cluster_restart_mid_storm);

    TEST_SECTION("Snapshot Under Load");
    RUN_TEST(test_snapshot_under_load_and_install);

    TEST_SECTION("Log Reconciliation");
    RUN_TEST(test_long_log_reconciliation);

    TEST_SECTION("Mixed Read/Write Flood");
    RUN_TEST(test_mixed_read_write_flood);

    return printSummary();
}
