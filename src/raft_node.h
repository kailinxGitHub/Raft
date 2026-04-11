#pragma once
#include <vector>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <string>
#include <set>
#include "config.h"
#include "log_entry.h"
#include "state_machine.h"
#include "network.h"
#include "timer.h"

// role enum for the node's state
enum class Role { Follower, Candidate, Leader };

// helper to convert role to string
std::string roleToString(Role r);

// event struct for the event queue
struct Event {
    enum Type { IncomingRPC, ElectionTimeout, HeartbeatTimeout };
    Type type;
    Message msg;
    int fromPeerId = -1;
    int lastSentIndex = -1;
};

// event queue class
class EventQueue {
    std::queue<Event> queue;
    std::mutex mtx;
    std::condition_variable cv;

// public methods
public:
    void push(Event e);
    Event pop();
};

// raft node class
class RaftNode {
    int nodeId;
    ClusterConfig config;

    // persistent state
    int currentTerm = 0;
    int votedFor = -1;
    std::vector<LogEntry> log;
    StateMachine kvStore;

    // snapshot state (persistent)
    int lastIncludedIndex = 0;
    int lastIncludedTerm  = 0;

    // volatile state for all servers
    int commitIndex = 0;
    int lastApplied = 0;
    int leaderId = -1;

    // volatile state for leader only
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;

    // role of the node
    Role role = Role::Follower;

    // infrastructure
    TCPServer server;
    Timer electionTimer;
    Timer heartbeatTimer;
    EventQueue eventQueue;
    std::mutex raftMtx;
    std::atomic<bool> running{false};

    // election bookkeeping
    int votesReceived = 0;
    int electionTerm = 0;

    // optional: fixed election timeout for split-vote testing
    int fixedTimeoutMs = 0;

    // read safety
    long long lastMajorityAckMs = 0;
    std::set<int> recentAckPeers;
    // check if majority of peers have acknowledged the heartbeat
    void checkMajorityAck();

    // raft protocol handlers
    Message handleRequestVote(const Message& msg);
    Message handleRequestVoteReply(const Message& msg);
    Message handleAppendEntries(const Message& msg);
    Message handleAppendEntriesReply(const Message& msg, int lastSentIndex);
    Message handleClientPut(const Message& msg);
    Message handleClientGet(const Message& msg);

    void startElection();
    void becomeFollower(int term);
    void becomeLeader();
    void sendHeartbeats();
    void sendAppendEntriesToPeer(int peerId);
    void advanceCommitIndex();
    void applyCommitted();

    int lastLogIndex() const;
    int lastLogTerm() const;
    bool isLogUpToDate(int candidateLastIndex, int candidateLastTerm) const;
    void log_event(const std::string& event) const;

    // log access helper (accounts for snapshot offset)
    const LogEntry& logAt(int logicalIndex) const;

    // persistent state (currentTerm, votedFor, log[])
    void savePersistentState() const;
    void appendLogEntryToDisk(const LogEntry& e) const;
    bool loadPersistentState();

    // snapshot management
    void takeSnapshot();
    void saveSnapshotToFile() const;
    bool loadSnapshotFromFile();
    std::string serializeKVStore() const;
    void deserializeKVStore(const std::string& data);

    // snapshot RPC handlers
    Message handleInstallSnapshot(const Message& msg);
    Message handleInstallSnapshotReply(const Message& msg);
    void    sendInstallSnapshotToPeer(int peerId);

public:
    // constructor
    RaftNode(int nodeId, const ClusterConfig& config, int fixedTimeout = 0);
    // run the node
    void run();
    // stop the node
    void stop();
};
