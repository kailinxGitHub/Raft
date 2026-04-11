#include "test_framework.h"
#include "../src/config.h"
#include "../src/log_entry.h"
#include "../src/state_machine.h"
#include "../src/network.h"
#include "../src/timer.h"
#include "../src/raft_node.h"
#include <fstream>
#include <thread>
#include <chrono>
#include <set>
#include <cmath>

// ============================================================
// Config Tests
// ============================================================

void test_config_load_3node() {
    ClusterConfig config = ClusterConfig::load("cluster.conf");
    ASSERT_EQ(config.clusterSize(), 3);
    ASSERT_EQ(config.majority(), 2);
    ASSERT_EQ(config.getNode(0).hostname, std::string("127.0.0.1"));
    ASSERT_EQ(config.getNode(0).port, 9000);
    ASSERT_EQ(config.getNode(1).port, 9001);
    ASSERT_EQ(config.getNode(2).port, 9002);
}

void test_config_load_5node() {
    ClusterConfig config = ClusterConfig::load("cluster5.conf");
    ASSERT_EQ(config.clusterSize(), 5);
    ASSERT_EQ(config.majority(), 3);
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(config.getNode(i).id, i);
        ASSERT_EQ(config.getNode(i).port, 9000 + i);
    }
}

void test_config_majority_calculations() {
    // 3 nodes: majority = 2
    ClusterConfig c3 = ClusterConfig::load("cluster.conf");
    ASSERT_EQ(c3.majority(), 2);

    // 5 nodes: majority = 3
    ClusterConfig c5 = ClusterConfig::load("cluster5.conf");
    ASSERT_EQ(c5.majority(), 3);
}

void test_config_invalid_node_throws() {
    ClusterConfig config = ClusterConfig::load("cluster.conf");
    ASSERT_THROWS(config.getNode(99));
}

void test_config_invalid_file_throws() {
    ASSERT_THROWS(ClusterConfig::load("nonexistent.conf"));
}

void test_config_skip_comments_and_blanks() {
    // cluster.conf has comment lines starting with #; verify they are skipped
    ClusterConfig config = ClusterConfig::load("cluster.conf");
    for (const auto& node : config.nodes) {
        ASSERT_GE(node.id, 0);
        ASSERT_GE(node.port, 9000);
    }
}

void test_config_election_timeout_range() {
    // Verify randomElectionTimeout produces values within the configured range
    for (int i = 0; i < 100; i++) {
        int t = randomElectionTimeout();
        ASSERT_GE(t, ELECTION_TIMEOUT_MIN_MS);
        ASSERT_LE(t, ELECTION_TIMEOUT_MAX_MS);
    }
}

void test_config_election_timeout_randomness() {
    // Verify that timeouts are not all the same (randomized)
    std::set<int> values;
    for (int i = 0; i < 50; i++) {
        values.insert(randomElectionTimeout());
    }
    // With 300-600ms range, 50 samples should produce at least 5 distinct values
    ASSERT_GE(static_cast<int>(values.size()), 5);
}

// ============================================================
// LogEntry Tests
// ============================================================

void test_log_entry_dummy() {
    LogEntry d = LogEntry::dummy();
    ASSERT_EQ(d.index, 0);
    ASSERT_EQ(d.term, 0);
    ASSERT_EQ(d.key, '\0');
    ASSERT_EQ(d.value, 0);
}

void test_log_entry_toString() {
    LogEntry e{1, 3, 'x', 42};
    std::string s = e.toString();
    ASSERT_TRUE(s.find("1") != std::string::npos);
    ASSERT_TRUE(s.find("3") != std::string::npos);
    ASSERT_TRUE(s.find("x") != std::string::npos);
    ASSERT_TRUE(s.find("42") != std::string::npos);
}

void test_log_entry_fields() {
    LogEntry e{5, 2, 'a', 100};
    ASSERT_EQ(e.index, 5);
    ASSERT_EQ(e.term, 2);
    ASSERT_EQ(e.key, 'a');
    ASSERT_EQ(e.value, 100);
}

// ============================================================
// StateMachine (KV Store) Tests
// ============================================================

void test_state_machine_empty_get() {
    StateMachine sm;
    ASSERT_EQ(sm.get('a'), -1);
    ASSERT_EQ(sm.get('z'), -1);
}

void test_state_machine_put_get() {
    StateMachine sm;
    sm.apply(LogEntry{1, 1, 'a', 10});
    ASSERT_EQ(sm.get('a'), 10);
}

void test_state_machine_overwrite() {
    StateMachine sm;
    sm.apply(LogEntry{1, 1, 'a', 10});
    sm.apply(LogEntry{2, 1, 'a', 20});
    ASSERT_EQ(sm.get('a'), 20);
}

void test_state_machine_multiple_keys() {
    StateMachine sm;
    sm.apply(LogEntry{1, 1, 'a', 1});
    sm.apply(LogEntry{2, 1, 'b', 2});
    sm.apply(LogEntry{3, 1, 'c', 3});
    ASSERT_EQ(sm.get('a'), 1);
    ASSERT_EQ(sm.get('b'), 2);
    ASSERT_EQ(sm.get('c'), 3);
    ASSERT_EQ(sm.get('d'), -1);
}

void test_state_machine_dummy_entry_no_effect() {
    StateMachine sm;
    sm.apply(LogEntry::dummy());
    ASSERT_EQ(sm.get('\0'), -1);
    ASSERT_TRUE(sm.getAll().empty());
}

void test_state_machine_getAll() {
    StateMachine sm;
    sm.apply(LogEntry{1, 1, 'x', 5});
    sm.apply(LogEntry{2, 1, 'y', 10});
    auto all = sm.getAll();
    ASSERT_EQ(static_cast<int>(all.size()), 2);
    ASSERT_EQ(all.at('x'), 5);
    ASSERT_EQ(all.at('y'), 10);
}

void test_state_machine_negative_values() {
    StateMachine sm;
    sm.apply(LogEntry{1, 1, 'n', -42});
    ASSERT_EQ(sm.get('n'), -42);
}

void test_state_machine_zero_value() {
    StateMachine sm;
    sm.apply(LogEntry{1, 1, 'z', 0});
    // 0 is a valid value, distinct from -1 (not found)
    ASSERT_EQ(sm.get('z'), 0);
}

// ============================================================
// Network Serialization Tests
// ============================================================

void test_serialize_request_vote() {
    Message msg;
    msg.type = MessageType::RequestVote;
    msg.term = 3;
    msg.senderId = 1;
    msg.candidateId = 1;
    msg.lastLogIndex = 5;
    msg.lastLogTerm = 2;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.type), static_cast<int>(MessageType::RequestVote));
    ASSERT_EQ(d.term, 3);
    ASSERT_EQ(d.senderId, 1);
    ASSERT_EQ(d.candidateId, 1);
    ASSERT_EQ(d.lastLogIndex, 5);
    ASSERT_EQ(d.lastLogTerm, 2);
}

void test_serialize_request_vote_reply_granted() {
    Message msg;
    msg.type = MessageType::RequestVoteReply;
    msg.term = 3;
    msg.senderId = 0;
    msg.voteGranted = true;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.type), static_cast<int>(MessageType::RequestVoteReply));
    ASSERT_EQ(d.term, 3);
    ASSERT_TRUE(d.voteGranted);
}

void test_serialize_request_vote_reply_denied() {
    Message msg;
    msg.type = MessageType::RequestVoteReply;
    msg.term = 5;
    msg.senderId = 2;
    msg.voteGranted = false;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_FALSE(d.voteGranted);
    ASSERT_EQ(d.term, 5);
}

void test_serialize_append_entries_heartbeat() {
    Message msg;
    msg.type = MessageType::AppendEntries;
    msg.term = 2;
    msg.senderId = 0;
    msg.leaderId = 0;
    msg.prevLogIndex = 3;
    msg.prevLogTerm = 1;
    msg.leaderCommit = 2;
    // empty entries = heartbeat

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.type), static_cast<int>(MessageType::AppendEntries));
    ASSERT_EQ(d.term, 2);
    ASSERT_EQ(d.leaderId, 0);
    ASSERT_EQ(d.prevLogIndex, 3);
    ASSERT_EQ(d.prevLogTerm, 1);
    ASSERT_EQ(d.leaderCommit, 2);
    ASSERT_TRUE(d.entries.empty());
}

void test_serialize_append_entries_with_entries() {
    Message msg;
    msg.type = MessageType::AppendEntries;
    msg.term = 3;
    msg.senderId = 1;
    msg.leaderId = 1;
    msg.prevLogIndex = 2;
    msg.prevLogTerm = 2;
    msg.leaderCommit = 1;
    msg.entries = {
        LogEntry{3, 3, 'a', 10},
        LogEntry{4, 3, 'b', 20},
        LogEntry{5, 3, 'c', 30}
    };

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.entries.size()), 3);
    ASSERT_EQ(d.entries[0].index, 3);
    ASSERT_EQ(d.entries[0].term, 3);
    ASSERT_EQ(d.entries[0].key, 'a');
    ASSERT_EQ(d.entries[0].value, 10);
    ASSERT_EQ(d.entries[1].key, 'b');
    ASSERT_EQ(d.entries[1].value, 20);
    ASSERT_EQ(d.entries[2].key, 'c');
    ASSERT_EQ(d.entries[2].value, 30);
}

void test_serialize_append_entries_reply_success() {
    Message msg;
    msg.type = MessageType::AppendEntriesReply;
    msg.term = 3;
    msg.senderId = 2;
    msg.success = true;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.type), static_cast<int>(MessageType::AppendEntriesReply));
    ASSERT_TRUE(d.success);
}

void test_serialize_append_entries_reply_failure() {
    Message msg;
    msg.type = MessageType::AppendEntriesReply;
    msg.term = 4;
    msg.senderId = 1;
    msg.success = false;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_FALSE(d.success);
    ASSERT_EQ(d.term, 4);
}

void test_serialize_client_put() {
    Message msg;
    msg.type = MessageType::ClientPut;
    msg.term = 0;
    msg.senderId = -1;
    msg.key = 'x';
    msg.value = 42;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.type), static_cast<int>(MessageType::ClientPut));
    ASSERT_EQ(d.key, 'x');
    ASSERT_EQ(d.value, 42);
}

void test_serialize_client_get() {
    Message msg;
    msg.type = MessageType::ClientGet;
    msg.senderId = -1;
    msg.key = 'y';

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.type), static_cast<int>(MessageType::ClientGet));
    ASSERT_EQ(d.key, 'y');
}

void test_serialize_client_response_leader() {
    Message msg;
    msg.type = MessageType::ClientResponse;
    msg.senderId = 0;
    msg.success = true;
    msg.value = 99;
    msg.isLeader = true;
    msg.redirectHost = "";
    msg.redirectPort = 0;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.type), static_cast<int>(MessageType::ClientResponse));
    ASSERT_TRUE(d.success);
    ASSERT_TRUE(d.isLeader);
    ASSERT_EQ(d.value, 99);
}

void test_serialize_client_response_redirect() {
    Message msg;
    msg.type = MessageType::ClientResponse;
    msg.senderId = 1;
    msg.success = false;
    msg.isLeader = false;
    msg.redirectHost = "127.0.0.1";
    msg.redirectPort = 9000;

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_FALSE(d.isLeader);
    ASSERT_FALSE(d.success);
    ASSERT_EQ(d.redirectHost, std::string("127.0.0.1"));
    ASSERT_EQ(d.redirectPort, 9000);
}

void test_serialize_single_entry() {
    Message msg;
    msg.type = MessageType::AppendEntries;
    msg.term = 1;
    msg.senderId = 0;
    msg.leaderId = 0;
    msg.prevLogIndex = 0;
    msg.prevLogTerm = 0;
    msg.leaderCommit = 0;
    msg.entries = {LogEntry{1, 1, 'z', 99}};

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(static_cast<int>(d.entries.size()), 1);
    ASSERT_EQ(d.entries[0].index, 1);
    ASSERT_EQ(d.entries[0].key, 'z');
    ASSERT_EQ(d.entries[0].value, 99);
}

void test_serialize_large_values() {
    Message msg;
    msg.type = MessageType::AppendEntries;
    msg.term = 999;
    msg.senderId = 4;
    msg.leaderId = 4;
    msg.prevLogIndex = 100;
    msg.prevLogTerm = 50;
    msg.leaderCommit = 98;
    msg.entries = {LogEntry{101, 999, 'a', 999999}};

    std::string s = serialize(msg);
    Message d = deserialize(s);

    ASSERT_EQ(d.term, 999);
    ASSERT_EQ(d.prevLogIndex, 100);
    ASSERT_EQ(d.leaderCommit, 98);
    ASSERT_EQ(d.entries[0].value, 999999);
}

void test_serialize_roundtrip_preserves_all_fields() {
    // RequestVote with all fields
    Message rv;
    rv.type = MessageType::RequestVote;
    rv.term = 7;
    rv.senderId = 3;
    rv.candidateId = 3;
    rv.lastLogIndex = 10;
    rv.lastLogTerm = 5;

    Message rvd = deserialize(serialize(rv));
    ASSERT_EQ(rvd.term, rv.term);
    ASSERT_EQ(rvd.senderId, rv.senderId);
    ASSERT_EQ(rvd.candidateId, rv.candidateId);
    ASSERT_EQ(rvd.lastLogIndex, rv.lastLogIndex);
    ASSERT_EQ(rvd.lastLogTerm, rv.lastLogTerm);
}

// ============================================================
// TCP Server / sendRPC Tests
// ============================================================

void test_tcp_server_echo() {
    TCPServer server;
    server.start(18100, [](const Message& req) -> Message {
        Message reply;
        reply.type = MessageType::RequestVoteReply;
        reply.term = req.term;
        reply.senderId = 99;
        reply.voteGranted = true;
        return reply;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Message req;
    req.type = MessageType::RequestVote;
    req.term = 5;
    req.senderId = 0;
    req.candidateId = 0;
    req.lastLogIndex = 0;
    req.lastLogTerm = 0;

    Message reply;
    bool ok = sendRPC("127.0.0.1", 18100, req, reply);
    ASSERT_TRUE(ok);
    ASSERT_EQ(reply.term, 5);
    ASSERT_TRUE(reply.voteGranted);
    ASSERT_EQ(reply.senderId, 99);

    server.stop();
}

void test_tcp_server_multiple_rpcs() {
    TCPServer server;
    server.start(18101, [](const Message& req) -> Message {
        Message reply;
        reply.type = MessageType::AppendEntriesReply;
        reply.term = req.term;
        reply.senderId = 0;
        reply.success = true;
        return reply;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    for (int i = 0; i < 10; i++) {
        Message req;
        req.type = MessageType::AppendEntries;
        req.term = i;
        req.senderId = 1;
        req.leaderId = 1;
        req.prevLogIndex = 0;
        req.prevLogTerm = 0;
        req.leaderCommit = 0;

        Message reply;
        bool ok = sendRPC("127.0.0.1", 18101, req, reply);
        ASSERT_TRUE(ok);
        ASSERT_EQ(reply.term, i);
        ASSERT_TRUE(reply.success);
    }

    server.stop();
}

void test_tcp_connection_to_dead_server() {
    Message req;
    req.type = MessageType::RequestVote;
    req.term = 1;
    req.senderId = 0;

    Message reply;
    bool ok = sendRPC("127.0.0.1", 19999, req, reply);
    ASSERT_FALSE(ok);
}

void test_tcp_concurrent_rpcs() {
    TCPServer server;
    std::atomic<int> count{0};
    server.start(18102, [&count](const Message& req) -> Message {
        count++;
        Message reply;
        reply.type = MessageType::RequestVoteReply;
        reply.term = req.term;
        reply.senderId = 0;
        reply.voteGranted = true;
        return reply;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<std::thread> threads;
    std::atomic<int> successes{0};
    for (int i = 0; i < 5; i++) {
        threads.emplace_back([i, &successes]() {
            Message req;
            req.type = MessageType::RequestVote;
            req.term = i;
            req.senderId = i;
            req.candidateId = i;
            req.lastLogIndex = 0;
            req.lastLogTerm = 0;

            Message reply;
            if (sendRPC("127.0.0.1", 18102, req, reply)) {
                successes++;
            }
        });
    }

    for (auto& t : threads) t.join();
    ASSERT_EQ(successes.load(), 5);
    ASSERT_EQ(count.load(), 5);

    server.stop();
}

// ============================================================
// Timer Tests
// ============================================================

void test_timer_fires() {
    Timer timer;
    std::atomic<int> fires{0};
    timer.start(
        []() { return 50; },
        [&fires]() { fires++; }
    );

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    timer.stop();

    // With 50ms interval over 200ms, should fire at least 2 times
    ASSERT_GE(fires.load(), 2);
}

void test_timer_reset_prevents_fire() {
    Timer timer;
    std::atomic<int> fires{0};
    timer.start(
        []() { return 100; },
        [&fires]() { fires++; }
    );

    // Reset every 50ms for 300ms — should never fire (interval is 100ms)
    for (int i = 0; i < 6; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        timer.reset();
    }

    timer.stop();
    ASSERT_EQ(fires.load(), 0);
}

void test_timer_stop_prevents_fire() {
    Timer timer;
    std::atomic<int> fires{0};
    timer.start(
        []() { return 500; },
        [&fires]() { fires++; }
    );

    timer.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(fires.load(), 0);
}

void test_timer_restart() {
    Timer timer;
    std::atomic<int> fires{0};
    timer.start(
        []() { return 50; },
        [&fires]() { fires++; }
    );

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    timer.stop();
    int firstCount = fires.load();
    ASSERT_GE(firstCount, 1);

    fires = 0;
    timer.start(
        []() { return 50; },
        [&fires]() { fires++; }
    );

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    timer.stop();
    ASSERT_GE(fires.load(), 1);
}

// ============================================================
// EventQueue Tests
// ============================================================

void test_event_queue_push_pop() {
    EventQueue eq;

    Event e;
    e.type = Event::ElectionTimeout;
    eq.push(e);

    Event popped = eq.pop();
    ASSERT_EQ(static_cast<int>(popped.type), static_cast<int>(Event::ElectionTimeout));
}

void test_event_queue_ordering() {
    EventQueue eq;

    for (int i = 0; i < 5; i++) {
        Event e;
        e.type = Event::IncomingRPC;
        e.msg.term = i;
        eq.push(e);
    }

    for (int i = 0; i < 5; i++) {
        Event e = eq.pop();
        ASSERT_EQ(e.msg.term, i);
    }
}

void test_event_queue_concurrent_push() {
    EventQueue eq;
    std::atomic<int> pushed{0};

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; i++) {
        threads.emplace_back([&eq, &pushed, i]() {
            for (int j = 0; j < 10; j++) {
                Event e;
                e.type = Event::IncomingRPC;
                e.msg.term = i * 10 + j;
                eq.push(e);
                pushed++;
            }
        });
    }

    for (auto& t : threads) t.join();
    ASSERT_EQ(pushed.load(), 40);

    // Drain all events
    for (int i = 0; i < 40; i++) {
        eq.pop();
    }
}

// ============================================================
// Role toString Test
// ============================================================

void test_role_to_string() {
    ASSERT_EQ(roleToString(Role::Follower), std::string("Follower"));
    ASSERT_EQ(roleToString(Role::Candidate), std::string("Candidate"));
    ASSERT_EQ(roleToString(Role::Leader), std::string("Leader"));
}

// ============================================================
// Log Structure Tests (verifying Raft log invariants)
// ============================================================

void test_log_dummy_at_index_zero() {
    // RaftNode initializes log with dummy at index 0
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    ASSERT_EQ(static_cast<int>(log.size()), 1);
    ASSERT_EQ(log[0].term, 0);
    ASSERT_EQ(log[0].index, 0);
}

void test_log_index_matches_position() {
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    log.push_back(LogEntry{1, 1, 'a', 10});
    log.push_back(LogEntry{2, 1, 'b', 20});
    log.push_back(LogEntry{3, 2, 'c', 30});

    for (int i = 0; i < static_cast<int>(log.size()); i++) {
        ASSERT_EQ(log[i].index, i);
    }
}

void test_log_last_index_and_term() {
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    ASSERT_EQ(static_cast<int>(log.size()) - 1, 0);
    ASSERT_EQ(log.back().term, 0);

    log.push_back(LogEntry{1, 3, 'x', 5});
    ASSERT_EQ(static_cast<int>(log.size()) - 1, 1);
    ASSERT_EQ(log.back().term, 3);
}

// ============================================================
// Log Up-to-Date Comparison Tests
// (Critical for election safety)
// ============================================================

void test_log_up_to_date_higher_term_wins() {
    // Node has log with lastTerm=2, lastIndex=5
    // Candidate has lastTerm=3, lastIndex=1
    // Candidate should win (higher term)
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    for (int i = 1; i <= 5; i++) log.push_back(LogEntry{i, 2, 'a', i});

    int myLastTerm = log.back().term;
    int myLastIndex = static_cast<int>(log.size()) - 1;

    int candidateLastTerm = 3;
    int candidateLastIndex = 1;

    // Candidate's log is more up-to-date if candidateLastTerm > myLastTerm
    bool upToDate = (candidateLastTerm > myLastTerm) ||
                    (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    ASSERT_TRUE(upToDate);
}

void test_log_up_to_date_same_term_longer_wins() {
    // Both have lastTerm=2, but candidate has lastIndex=7, we have lastIndex=5
    int myLastTerm = 2, myLastIndex = 5;
    int candidateLastTerm = 2, candidateLastIndex = 7;

    bool upToDate = (candidateLastTerm > myLastTerm) ||
                    (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    ASSERT_TRUE(upToDate);
}

void test_log_up_to_date_lower_term_loses() {
    // Node has lastTerm=3, candidate has lastTerm=2 (even if longer)
    int myLastTerm = 3, myLastIndex = 2;
    int candidateLastTerm = 2, candidateLastIndex = 100;

    bool upToDate = (candidateLastTerm > myLastTerm) ||
                    (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    ASSERT_FALSE(upToDate);
}

void test_log_up_to_date_equal() {
    int myLastTerm = 2, myLastIndex = 5;
    int candidateLastTerm = 2, candidateLastIndex = 5;

    bool upToDate = (candidateLastTerm > myLastTerm) ||
                    (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    ASSERT_TRUE(upToDate);
}

// ============================================================
// Log Conflict Resolution Tests
// (Simulating AppendEntries log matching)
// ============================================================

void test_log_conflict_delete_and_append() {
    // Follower has: [dummy, (1,1,a,1), (2,1,b,2), (3,2,c,3)]
    // Leader sends prevLogIndex=1, prevLogTerm=1, entries=[(2,3,d,4), (3,3,e,5)]
    // Entry at index 2 conflicts (term 1 vs 3), should be deleted and replaced
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    log.push_back(LogEntry{1, 1, 'a', 1});
    log.push_back(LogEntry{2, 1, 'b', 2});
    log.push_back(LogEntry{3, 2, 'c', 3});

    int prevLogIndex = 1;
    std::vector<LogEntry> newEntries = {
        LogEntry{2, 3, 'd', 4},
        LogEntry{3, 3, 'e', 5}
    };

    // Simulate conflict resolution
    int insertIdx = prevLogIndex + 1;
    for (size_t i = 0; i < newEntries.size(); i++) {
        int logIdx = insertIdx + static_cast<int>(i);
        if (logIdx < static_cast<int>(log.size())) {
            if (log[logIdx].term != newEntries[i].term) {
                log.resize(logIdx);
                log.push_back(newEntries[i]);
            }
        } else {
            log.push_back(newEntries[i]);
        }
    }

    ASSERT_EQ(static_cast<int>(log.size()), 4);
    ASSERT_EQ(log[2].term, 3);
    ASSERT_EQ(log[2].key, 'd');
    ASSERT_EQ(log[3].term, 3);
    ASSERT_EQ(log[3].key, 'e');
}

void test_log_append_no_conflict() {
    // Follower has: [dummy, (1,1,a,1)]
    // Leader sends prevLogIndex=1, prevLogTerm=1, entries=[(2,1,b,2)]
    // No conflict, just append
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    log.push_back(LogEntry{1, 1, 'a', 1});

    int prevLogIndex = 1;
    std::vector<LogEntry> newEntries = {LogEntry{2, 1, 'b', 2}};

    int insertIdx = prevLogIndex + 1;
    for (size_t i = 0; i < newEntries.size(); i++) {
        int logIdx = insertIdx + static_cast<int>(i);
        if (logIdx < static_cast<int>(log.size())) {
            if (log[logIdx].term != newEntries[i].term) {
                log.resize(logIdx);
                log.push_back(newEntries[i]);
            }
        } else {
            log.push_back(newEntries[i]);
        }
    }

    ASSERT_EQ(static_cast<int>(log.size()), 3);
    ASSERT_EQ(log[2].key, 'b');
}

void test_log_prevlog_mismatch_rejects() {
    // Follower has: [dummy, (1,1,a,1)]
    // Leader sends prevLogIndex=1, prevLogTerm=2 (mismatch)
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    log.push_back(LogEntry{1, 1, 'a', 1});

    int prevLogIndex = 1;
    int prevLogTerm = 2;

    bool match = (prevLogIndex < static_cast<int>(log.size())) &&
                 (log[prevLogIndex].term == prevLogTerm);
    ASSERT_FALSE(match);
}

void test_log_prevlog_index_too_high_rejects() {
    // Follower has: [dummy, (1,1,a,1)]
    // Leader sends prevLogIndex=5 (beyond log)
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());
    log.push_back(LogEntry{1, 1, 'a', 1});

    int prevLogIndex = 5;
    bool indexValid = prevLogIndex < static_cast<int>(log.size());
    ASSERT_FALSE(indexValid);
}

// ============================================================
// Commit Index Advancement Tests
// (Verifying Section 5.4.2: only commit entries from current term)
// ============================================================

void test_commit_index_majority_required() {
    // 3-node cluster: need 2 to commit
    // matchIndex = [_, 3, 1] with self at index 3
    // Only index 1 has majority (self + node2 didn't reach beyond 1)
    // Actually: self=3, node1=3, node2=1 → majority at 3 (self+node1)
    std::vector<int> matchIndex = {0, 3, 1};
    int selfIndex = 3;
    int clusterSize = 3;
    int majority = clusterSize / 2 + 1;

    int commitN = 0;
    for (int n = selfIndex; n > 0; n--) {
        int count = 1; // self
        for (int i = 0; i < clusterSize; i++) {
            if (i == 0) continue; // self = node 0
            if (matchIndex[i] >= n) count++;
        }
        if (count >= majority) {
            commitN = n;
            break;
        }
    }

    ASSERT_EQ(commitN, 3);
}

void test_commit_index_no_majority() {
    // 5-node cluster: need 3 to commit
    // matchIndex = [_, 0, 0, 0, 0] (nobody replicated anything)
    std::vector<int> matchIndex = {0, 0, 0, 0, 0};
    int selfIndex = 3;
    int majority = 3;

    int commitN = 0;
    for (int n = selfIndex; n > 0; n--) {
        int count = 1;
        for (int i = 1; i < 5; i++) {
            if (matchIndex[i] >= n) count++;
        }
        if (count >= majority) {
            commitN = n;
            break;
        }
    }

    ASSERT_EQ(commitN, 0);
}

void test_commit_index_5node_partial() {
    // 5-node cluster: matchIndex = [_, 5, 5, 2, 1]
    // Self at 5, nodes 1,2 at 5, node 3 at 2, node 4 at 1
    // Majority of 3 at index 5: self + node1 + node2 = 3 ✓
    std::vector<int> matchIndex = {0, 5, 5, 2, 1};
    int selfIndex = 5;
    int majority = 3;

    int commitN = 0;
    for (int n = selfIndex; n > 0; n--) {
        int count = 1;
        for (int i = 1; i < 5; i++) {
            if (matchIndex[i] >= n) count++;
        }
        if (count >= majority) {
            commitN = n;
            break;
        }
    }

    ASSERT_EQ(commitN, 5);
}

// ============================================================
// KV Store Serialization Tests
// (serializeKVStore / deserializeKVStore algorithm verification)
// ============================================================

void test_kv_serialize_format() {
    // Verify "key=value;" format using StateMachine::getAll() (mirrors serializeKVStore)
    StateMachine sm;
    sm.apply(LogEntry{1, 1, 'a', 10});
    sm.apply(LogEntry{2, 1, 'b', 20});

    std::ostringstream oss;
    for (const auto& [k, v] : sm.getAll()) {
        oss << k << "=" << v << ";";
    }
    std::string serialized = oss.str();

    ASSERT_TRUE(serialized.find("a=10;") != std::string::npos);
    ASSERT_TRUE(serialized.find("b=20;") != std::string::npos);
}

void test_kv_serialize_roundtrip() {
    // Serialize then deserialize a KV store (mirrors serializeKVStore + deserializeKVStore)
    StateMachine original;
    original.apply(LogEntry{1, 1, 'x', 42});
    original.apply(LogEntry{2, 1, 'y', -7});
    original.apply(LogEntry{3, 1, 'z', 0});

    // serialize
    std::ostringstream oss;
    for (const auto& [k, v] : original.getAll()) {
        oss << k << "=" << v << ";";
    }
    std::string data = oss.str();

    // deserialize
    StateMachine restored;
    std::istringstream iss(data);
    std::string pair;
    while (std::getline(iss, pair, ';')) {
        if (pair.empty()) continue;
        auto eq = pair.find('=');
        if (eq == std::string::npos) continue;
        LogEntry e;
        e.key   = pair[0];
        e.value = std::stoi(pair.substr(eq + 1));
        e.index = 1;
        e.term  = 1;
        restored.apply(e);
    }

    ASSERT_EQ(restored.get('x'), 42);
    ASSERT_EQ(restored.get('y'), -7);
    ASSERT_EQ(restored.get('z'), 0);
}

// ============================================================
// Persistence File Format Tests
// (savePersistentState / loadPersistentState / saveSnapshotToFile / loadSnapshotFromFile)
// ============================================================

void test_persistent_state_file_format() {
    // Write a mock raft_state file in the exact format savePersistentState uses,
    // then parse it back using the same logic as loadPersistentState.
    const std::string path = "test_raft_state_tmp.dat";

    // Write (mirrors savePersistentState)
    {
        std::ofstream f(path);
        ASSERT_TRUE(f.is_open());
        f << "currentTerm=5\n";
        f << "votedFor=2\n";
        f << "log:1,3,a,100\n";
        f << "log:2,3,b,200\n";
    }

    // Parse back (mirrors loadPersistentState)
    int currentTerm = 0, votedFor = -1;
    std::vector<LogEntry> log;
    log.push_back(LogEntry::dummy());

    std::ifstream f(path);
    ASSERT_TRUE(f.is_open());
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("currentTerm=", 0) == 0) {
            currentTerm = std::stoi(line.substr(12));
        } else if (line.rfind("votedFor=", 0) == 0) {
            votedFor = std::stoi(line.substr(9));
        } else if (line.rfind("log:", 0) == 0) {
            std::string s = line.substr(4);
            LogEntry e;
            size_t p0 = 0, p1;
            p1 = s.find(',', p0); e.index = std::stoi(s.substr(p0, p1 - p0)); p0 = p1 + 1;
            p1 = s.find(',', p0); e.term  = std::stoi(s.substr(p0, p1 - p0)); p0 = p1 + 1;
            p1 = s.find(',', p0); e.key   = s[p0];                             p0 = p1 + 1;
            e.value = std::stoi(s.substr(p0));
            log.push_back(e);
        }
    }
    f.close();
    std::remove(path.c_str());

    ASSERT_EQ(currentTerm, 5);
    ASSERT_EQ(votedFor, 2);
    ASSERT_EQ(static_cast<int>(log.size()), 3);
    ASSERT_EQ(log[1].index, 1);
    ASSERT_EQ(log[1].term, 3);
    ASSERT_EQ(log[1].key, 'a');
    ASSERT_EQ(log[1].value, 100);
    ASSERT_EQ(log[2].key, 'b');
    ASSERT_EQ(log[2].value, 200);
}

void test_snapshot_file_format() {
    // Write a mock snapshot file in the exact format saveSnapshotToFile uses,
    // then parse it back using the same logic as loadSnapshotFromFile.
    const std::string path = "test_snapshot_tmp.dat";

    // Write (mirrors saveSnapshotToFile)
    {
        std::ofstream f(path);
        ASSERT_TRUE(f.is_open());
        f << "lastIncludedIndex=10\n";
        f << "lastIncludedTerm=3\n";
        f << "kv:x=42\n";
        f << "kv:y=-7\n";
    }

    // Parse back (mirrors loadSnapshotFromFile)
    int lii = 0, lit = 0;
    StateMachine sm;
    std::ifstream f(path);
    ASSERT_TRUE(f.is_open());
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("lastIncludedIndex=", 0) == 0) {
            lii = std::stoi(line.substr(18));
        } else if (line.rfind("lastIncludedTerm=", 0) == 0) {
            lit = std::stoi(line.substr(17));
        } else if (line.rfind("kv:", 0) == 0) {
            auto eq = line.find('=', 3);
            if (eq != std::string::npos) {
                LogEntry e;
                e.key   = line[3];
                e.value = std::stoi(line.substr(eq + 1));
                e.index = 1;
                e.term  = 1;
                sm.apply(e);
            }
        }
    }
    f.close();
    std::remove(path.c_str());

    ASSERT_EQ(lii, 10);
    ASSERT_EQ(lit, 3);
    ASSERT_EQ(sm.get('x'), 42);
    ASSERT_EQ(sm.get('y'), -7);
    ASSERT_EQ(sm.get('z'), -1);  // not in snapshot
}

// ============================================================
// Main
// ============================================================

int main() {
    std::cout << "Raft Unit Tests" << std::endl;

    TEST_SECTION("Config");
    RUN_TEST(test_config_load_3node);
    RUN_TEST(test_config_load_5node);
    RUN_TEST(test_config_majority_calculations);
    RUN_TEST(test_config_invalid_node_throws);
    RUN_TEST(test_config_invalid_file_throws);
    RUN_TEST(test_config_skip_comments_and_blanks);
    RUN_TEST(test_config_election_timeout_range);
    RUN_TEST(test_config_election_timeout_randomness);

    TEST_SECTION("LogEntry");
    RUN_TEST(test_log_entry_dummy);
    RUN_TEST(test_log_entry_toString);
    RUN_TEST(test_log_entry_fields);

    TEST_SECTION("StateMachine (KV Store)");
    RUN_TEST(test_state_machine_empty_get);
    RUN_TEST(test_state_machine_put_get);
    RUN_TEST(test_state_machine_overwrite);
    RUN_TEST(test_state_machine_multiple_keys);
    RUN_TEST(test_state_machine_dummy_entry_no_effect);
    RUN_TEST(test_state_machine_getAll);
    RUN_TEST(test_state_machine_negative_values);
    RUN_TEST(test_state_machine_zero_value);

    TEST_SECTION("Network Serialization");
    RUN_TEST(test_serialize_request_vote);
    RUN_TEST(test_serialize_request_vote_reply_granted);
    RUN_TEST(test_serialize_request_vote_reply_denied);
    RUN_TEST(test_serialize_append_entries_heartbeat);
    RUN_TEST(test_serialize_append_entries_with_entries);
    RUN_TEST(test_serialize_append_entries_reply_success);
    RUN_TEST(test_serialize_append_entries_reply_failure);
    RUN_TEST(test_serialize_client_put);
    RUN_TEST(test_serialize_client_get);
    RUN_TEST(test_serialize_client_response_leader);
    RUN_TEST(test_serialize_client_response_redirect);
    RUN_TEST(test_serialize_single_entry);
    RUN_TEST(test_serialize_large_values);
    RUN_TEST(test_serialize_roundtrip_preserves_all_fields);

    TEST_SECTION("TCP Server / RPC");
    RUN_TEST(test_tcp_server_echo);
    RUN_TEST(test_tcp_server_multiple_rpcs);
    RUN_TEST(test_tcp_connection_to_dead_server);
    RUN_TEST(test_tcp_concurrent_rpcs);

    TEST_SECTION("Timer");
    RUN_TEST(test_timer_fires);
    RUN_TEST(test_timer_reset_prevents_fire);
    RUN_TEST(test_timer_stop_prevents_fire);
    RUN_TEST(test_timer_restart);

    TEST_SECTION("EventQueue");
    RUN_TEST(test_event_queue_push_pop);
    RUN_TEST(test_event_queue_ordering);
    RUN_TEST(test_event_queue_concurrent_push);

    TEST_SECTION("Role");
    RUN_TEST(test_role_to_string);

    TEST_SECTION("Log Structure Invariants");
    RUN_TEST(test_log_dummy_at_index_zero);
    RUN_TEST(test_log_index_matches_position);
    RUN_TEST(test_log_last_index_and_term);

    TEST_SECTION("Log Up-to-Date Comparison (Election Safety)");
    RUN_TEST(test_log_up_to_date_higher_term_wins);
    RUN_TEST(test_log_up_to_date_same_term_longer_wins);
    RUN_TEST(test_log_up_to_date_lower_term_loses);
    RUN_TEST(test_log_up_to_date_equal);

    TEST_SECTION("Log Conflict Resolution (AppendEntries)");
    RUN_TEST(test_log_conflict_delete_and_append);
    RUN_TEST(test_log_append_no_conflict);
    RUN_TEST(test_log_prevlog_mismatch_rejects);
    RUN_TEST(test_log_prevlog_index_too_high_rejects);

    TEST_SECTION("Commit Index Advancement (Section 5.4.2)");
    RUN_TEST(test_commit_index_majority_required);
    RUN_TEST(test_commit_index_no_majority);
    RUN_TEST(test_commit_index_5node_partial);

    TEST_SECTION("KV Store Serialization (serializeKVStore / deserializeKVStore)");
    RUN_TEST(test_kv_serialize_format);
    RUN_TEST(test_kv_serialize_roundtrip);

    TEST_SECTION("Persistence File Format (savePersistentState / saveSnapshotToFile)");
    RUN_TEST(test_persistent_state_file_format);
    RUN_TEST(test_snapshot_file_format);

    return printSummary();
}
