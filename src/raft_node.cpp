#include "raft_node.h"
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <sstream>
#include <iomanip>

// helper to convert role to string
std::string roleToString(Role r) {
    switch (r) {
        case Role::Follower:  return "Follower";
        case Role::Candidate: return "Candidate";
        case Role::Leader:    return "Leader";
    }
    return "Unknown";
}

// event queue class
void EventQueue::push(Event e) {
    std::lock_guard<std::mutex> lock(mtx);
    queue.push(std::move(e));
    cv.notify_one();
}

// pop event from the queue
Event EventQueue::pop() {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return !queue.empty(); });
    Event e = std::move(queue.front());
    queue.pop();
    return e;
}

// constructor
RaftNode::RaftNode(int nodeId, const ClusterConfig& config, int fixedTimeout)
    : nodeId(nodeId), config(config), fixedTimeoutMs(fixedTimeout) {
    log.push_back(LogEntry::dummy());
}

// log event
void RaftNode::log_event(const std::string& event) const {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count() % 100000;
    std::cerr << "[" << std::setfill('0') << std::setw(5) << ms << "]"
              << "[Node " << nodeId << "]"
              << "[" << roleToString(role) << "]"
              << "[T" << currentTerm << "] "
              << event << std::endl;
}

// get last log index (accounts for snapshot offset)
int RaftNode::lastLogIndex() const {
    return lastIncludedIndex + static_cast<int>(log.size()) - 1;
}

// log access helper: returns entry at logical index i
const LogEntry& RaftNode::logAt(int logicalIndex) const {
    return log[logicalIndex - lastIncludedIndex];
}

// get last log term
int RaftNode::lastLogTerm() const {
    return log.back().term;
}

// check if log is up to date
bool RaftNode::isLogUpToDate(int candidateLastIndex, int candidateLastTerm) const {
    if (candidateLastTerm != lastLogTerm()) {
        return candidateLastTerm > lastLogTerm();
    }
    return candidateLastIndex >= lastLogIndex();
}

void RaftNode::becomeFollower(int term) {
    Role oldRole = role;
    role = Role::Follower;

    // only clear votedFor when the term actually advances
    if (term > currentTerm) {
        currentTerm = term;
        votedFor = -1;
        savePersistentState();
    }

    // stop heartbeat and election timers
    heartbeatTimer.stop();
    electionTimer.reset();
    if (oldRole != Role::Follower) {
        log_event("Stepped down to Follower");
    }
}

// start election
void RaftNode::startElection() {
    currentTerm++;
    role = Role::Candidate;
    votedFor = nodeId;
    votesReceived = 1;
    electionTerm = currentTerm;
    // Note: we do NOT persist here. A self-vote in a failed candidacy must not
    // survive a crash — doing so causes stale high-term disruption on restart.
    // We persist when granting a vote to someone else (handleRequestVote) or
    // when our term advances due to a higher-term message (becomeFollower).
    electionTimer.reset();

    log_event("Starting election for term " + std::to_string(currentTerm));

    int term = currentTerm;
    int candidateId = nodeId;
    int lli = lastLogIndex();
    int llt = lastLogTerm();

    // send request vote to all other nodes
    for (const auto& node : config.nodes) {
        if (node.id == nodeId) continue;

        std::thread([this, node, term, candidateId, lli, llt]() {
            Message req;
            req.type = MessageType::RequestVote;
            req.term = term;
            req.senderId = candidateId;
            req.candidateId = candidateId;
            req.lastLogIndex = lli;
            req.lastLogTerm = llt;

            Message reply;
            if (sendRPC(node.hostname, node.port, req, reply)) {
                reply.senderId = node.id;
                eventQueue.push(Event{Event::IncomingRPC, reply, node.id});
            }
        }).detach();
    }
}

// check if majority acknowledged the heartbeat
void RaftNode::checkMajorityAck() {
    int ackCount = 1 + static_cast<int>(recentAckPeers.size());
    if (ackCount >= config.majority()) {
        lastMajorityAckMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
}

// become leader
void RaftNode::becomeLeader() {
    role = Role::Leader;
    leaderId = nodeId;

    // initialize nextIndex and matchIndex
    nextIndex.assign(config.clusterSize(), lastLogIndex() + 1);
    matchIndex.assign(config.clusterSize(), 0);
    recentAckPeers.clear();
    // reset last majority acknowledgement time
    lastMajorityAckMs = 0;

    // stop election timer
    electionTimer.stop();

    log_event("Became Leader for term " + std::to_string(currentTerm));

    // start heartbeat timer
    heartbeatTimer.start(
        []() { return HEARTBEAT_INTERVAL_MS; },
        [this]() { eventQueue.push(Event{Event::HeartbeatTimeout, {}, -1}); }
    );

    sendHeartbeats();
}

// send heartbeats to all other nodes
void RaftNode::sendHeartbeats() {
    recentAckPeers.clear();
    for (const auto& node : config.nodes) {
        if (node.id == nodeId) continue;
        sendAppendEntriesToPeer(node.id);
    }
}

// send append entries to a peer (or InstallSnapshot if log is compacted past peer)
void RaftNode::sendAppendEntriesToPeer(int peerId) {
    if (role != Role::Leader) return;

    int ni = nextIndex[peerId];

    // If the entry the follower needs is before our snapshot, send snapshot instead
    if (ni - 1 < lastIncludedIndex) {
        sendInstallSnapshotToPeer(peerId);
        return;
    }

    int prevIdx = ni - 1;
    int prevTrm;
    if (prevIdx == lastIncludedIndex) {
        prevTrm = lastIncludedTerm;
    } else if (prevIdx > lastIncludedIndex &&
               (prevIdx - lastIncludedIndex) < static_cast<int>(log.size())) {
        prevTrm = log[prevIdx - lastIncludedIndex].term;
    } else {
        prevTrm = 0;
    }

    std::vector<LogEntry> entriesToSend;
    for (int i = ni; i <= lastLogIndex(); i++) {
        entriesToSend.push_back(logAt(i));
    }

    int term    = currentTerm;
    int lid     = nodeId;
    int lc      = commitIndex;
    int lastIdx = lastLogIndex();
    const auto& peerConfig = config.getNode(peerId);

    std::thread([this, peerConfig, term, lid, prevIdx, prevTrm, lc,
                 entriesToSend, peerId, lastIdx]() {
        Message req;
        req.type         = MessageType::AppendEntries;
        req.term         = term;
        req.senderId     = lid;
        req.leaderId     = lid;
        req.prevLogIndex = prevIdx;
        req.prevLogTerm  = prevTrm;
        req.leaderCommit = lc;
        req.entries      = entriesToSend;

        Message reply;
        if (sendRPC(peerConfig.hostname, peerConfig.port, req, reply)) {
            reply.senderId = peerId;
            eventQueue.push(Event{Event::IncomingRPC, reply, peerId, lastIdx});
        }
    }).detach();
}

// handle request vote
Message RaftNode::handleRequestVote(const Message& msg) {
    Message reply;
    reply.type = MessageType::RequestVoteReply;
    reply.senderId = nodeId;
    reply.term = currentTerm;
    reply.voteGranted = false;

    if (msg.term > currentTerm) {
        becomeFollower(msg.term);
        reply.term = currentTerm;
    }

    if (msg.term < currentTerm) {
        log_event("Rejected vote for Node " + std::to_string(msg.candidateId)
                  + " (stale term " + std::to_string(msg.term) + ")");
        return reply;
    }

    bool canVote = (votedFor == -1 || votedFor == msg.candidateId);
    bool logOk = isLogUpToDate(msg.lastLogIndex, msg.lastLogTerm);

    if (canVote && logOk) {
        votedFor = msg.candidateId;
        savePersistentState();
        reply.voteGranted = true;
        electionTimer.reset();
        log_event("Voted for Node " + std::to_string(msg.candidateId));
    } else {
        log_event("Rejected vote for Node " + std::to_string(msg.candidateId)
                  + " (canVote=" + (canVote ? "true" : "false")
                  + ", logOk=" + (logOk ? "true" : "false") + ")");
    }

    return reply;
}

// handle request vote reply
Message RaftNode::handleRequestVoteReply(const Message& msg) {
    if (role != Role::Candidate || msg.term != electionTerm) {
        return {};
    }

    if (msg.term > currentTerm) {
        becomeFollower(msg.term);
        return {};
    }

    if (msg.voteGranted) {
        votesReceived++;
        log_event("Received vote from Node " + std::to_string(msg.senderId)
                  + " (" + std::to_string(votesReceived) + "/"
                  + std::to_string(config.majority()) + " needed)");
        if (votesReceived >= config.majority()) {
            becomeLeader();
        }
    }

    return {};
}

// handle append entries
Message RaftNode::handleAppendEntries(const Message& msg) {
    Message reply;
    reply.type = MessageType::AppendEntriesReply;
    reply.senderId = nodeId;
    reply.term = currentTerm;
    reply.success = false;

    if (msg.term < currentTerm) {
        log_event("Rejected AppendEntries from Node " + std::to_string(msg.leaderId)
                  + " (stale term " + std::to_string(msg.term) + ")");
        return reply;
    }

    if (msg.term >= currentTerm) {
        if (msg.term > currentTerm || role != Role::Follower) {
            becomeFollower(msg.term);
        }
        leaderId = msg.leaderId;
        electionTimer.reset();
    }

    reply.term = currentTerm;

    // check prevLog match
    if (msg.prevLogIndex > 0) {
        int prevPos = msg.prevLogIndex - lastIncludedIndex;
        if (msg.prevLogIndex > lastIncludedIndex &&
            prevPos >= static_cast<int>(log.size())) {
            log_event("AppendEntries log mismatch: prevLogIndex "
                      + std::to_string(msg.prevLogIndex) + " beyond log");
            return reply;
        }
        int prevTrm = (msg.prevLogIndex == lastIncludedIndex)
                      ? lastIncludedTerm
                      : log[prevPos].term;
        if (prevTrm != msg.prevLogTerm) {
            log_event("AppendEntries term mismatch at index "
                      + std::to_string(msg.prevLogIndex));
            return reply;
        }
    }

    // delete conflicting entries and append new ones
    int insertLogicalIdx = msg.prevLogIndex + 1;
    bool truncated = false;
    std::vector<LogEntry> newEntries;  // entries that were genuinely appended
    for (size_t i = 0; i < msg.entries.size(); i++) {
        int logicalIdx = insertLogicalIdx + static_cast<int>(i);
        int arrPos = logicalIdx - lastIncludedIndex;
        if (arrPos < static_cast<int>(log.size())) {
            if (log[arrPos].term != msg.entries[i].term) {
                log.resize(arrPos);
                log.push_back(msg.entries[i]);
                truncated = true;
                newEntries.push_back(msg.entries[i]);
            }
            // matching entry already present — no action needed
        } else {
            log.push_back(msg.entries[i]);
            newEntries.push_back(msg.entries[i]);
        }
    }

    if (!msg.entries.empty()) {
        if (truncated) {
            // full rewrite needed because we deleted entries
            savePersistentState();
        } else {
            // pure appends — O(1) incremental write per new entry
            for (const auto& e : newEntries) {
                appendLogEntryToDisk(e);
            }
        }
    }

    if (msg.leaderCommit > commitIndex) {
        commitIndex = std::min(msg.leaderCommit, lastLogIndex());
    }

    reply.success = true;

    if (!msg.entries.empty()) {
        log_event("Appended " + std::to_string(msg.entries.size())
                  + " entries, commitIndex=" + std::to_string(commitIndex));
    }

    applyCommitted();

    return reply;
}

// handle append entries reply
Message RaftNode::handleAppendEntriesReply(const Message& msg, int lastSentIndex) {
    if (role != Role::Leader) return {};

    if (msg.term > currentTerm) {
        becomeFollower(msg.term);
        return {};
    }

    int peerId = msg.senderId;

    if (msg.success) {
        recentAckPeers.insert(peerId);
        checkMajorityAck();

        // update nextIndex and matchIndex
        if (lastSentIndex >= nextIndex[peerId]) {
            nextIndex[peerId] = lastSentIndex + 1;
            matchIndex[peerId] = lastSentIndex;
            log_event("Node " + std::to_string(peerId) + " matched up to index "
                      + std::to_string(matchIndex[peerId]));
        }
        advanceCommitIndex();
    } else {
        if (nextIndex[peerId] > 1) {
            nextIndex[peerId]--;
            log_event("Decremented nextIndex for Node " + std::to_string(peerId)
                      + " to " + std::to_string(nextIndex[peerId]));
            sendAppendEntriesToPeer(peerId);
        }
    }

    return {};
}

// advance commit index
void RaftNode::advanceCommitIndex() {
    for (int n = lastLogIndex(); n > commitIndex; n--) {
        if (logAt(n).term != currentTerm) continue;

        int count = 1;
        for (const auto& node : config.nodes) {
            if (node.id == nodeId) continue;
            if (matchIndex[node.id] >= n) count++;
        }

        if (count >= config.majority()) {
            log_event("Advanced commitIndex from " + std::to_string(commitIndex)
                      + " to " + std::to_string(n));
            commitIndex = n;
            break;
        }
    }
}

// apply committed entries
void RaftNode::applyCommitted() {
    while (lastApplied < commitIndex) {
        lastApplied++;
        const LogEntry& e = logAt(lastApplied);
        kvStore.apply(e);
        log_event("Applied log[" + std::to_string(lastApplied) + "]: "
                  + std::string(1, e.key) + "=" + std::to_string(e.value));
    }
    // trigger snapshot when log has grown too large
    if (static_cast<int>(log.size()) > SNAPSHOT_LOG_THRESHOLD) {
        takeSnapshot();
    }
}

// serialize KV store to "A=1;B=2;" format (no pipes — safe inside message fields)
std::string RaftNode::serializeKVStore() const {
    std::ostringstream oss;
    for (const auto& [k, v] : kvStore.getAll()) {
        oss << k << "=" << v << ";";
    }
    return oss.str();
}

// deserialize KV store from "A=1;B=2;" format into kvStore
void RaftNode::deserializeKVStore(const std::string& data) {
    std::istringstream iss(data);
    std::string pair;
    while (std::getline(iss, pair, ';')) {
        if (pair.empty()) continue;
        auto eq = pair.find('=');
        if (eq == std::string::npos) continue;
        LogEntry e;
        e.key   = pair[0];
        e.value = std::stoi(pair.substr(eq + 1));
        e.index = 0;
        e.term  = 0;
        kvStore.apply(e);
    }
}

// take a snapshot at lastApplied: compact log and save to disk
void RaftNode::takeSnapshot() {
    if (lastApplied <= lastIncludedIndex) return;

    int newLII = lastApplied;
    int newLIT = logAt(newLII).term;
    int lli    = lastLogIndex();

    // build new log: sentinel at newLII + any uncommitted tail entries
    std::vector<LogEntry> newLog;
    newLog.push_back({newLII, newLIT, '\0', 0});
    for (int i = newLII + 1; i <= lli; i++) {
        newLog.push_back(logAt(i));
    }

    int oldSize = static_cast<int>(log.size());
    lastIncludedIndex = newLII;
    lastIncludedTerm  = newLIT;
    log = std::move(newLog);

    saveSnapshotToFile();
    savePersistentState();  // update persistent log to reflect compaction
    log_event("Snapshot taken at index=" + std::to_string(lastIncludedIndex)
              + " log shrunk from " + std::to_string(oldSize)
              + " to " + std::to_string((int)log.size()) + " entries");
}

// save persistent Raft state (currentTerm, votedFor, log[]) to raft_state_<nodeId>.dat
// Full rewrite — used when term/vote changes, log is truncated, or snapshot is taken.
void RaftNode::savePersistentState() const {
    std::string path = "raft_state_" + std::to_string(nodeId) + ".dat";
    std::ofstream f(path);
    if (!f.is_open()) return;
    f << "currentTerm=" << currentTerm << "\n";
    f << "votedFor="    << votedFor    << "\n";
    // write log entries starting from index 1 (skip the sentinel at 0)
    for (size_t i = 1; i < log.size(); i++) {
        const LogEntry& e = log[i];
        f << "log:" << e.index << "," << e.term << "," << e.key << "," << e.value << "\n";
    }
}

// append a single log entry to the persistent state file (O(1) — no full rewrite)
void RaftNode::appendLogEntryToDisk(const LogEntry& e) const {
    std::string path = "raft_state_" + std::to_string(nodeId) + ".dat";
    std::ofstream f(path, std::ios::app);
    if (!f.is_open()) return;
    f << "log:" << e.index << "," << e.term << "," << e.key << "," << e.value << "\n";
}

// load persistent Raft state from raft_state_<nodeId>.dat; returns true if loaded
bool RaftNode::loadPersistentState() {
    std::string path = "raft_state_" + std::to_string(nodeId) + ".dat";
    std::ifstream f(path);
    if (!f.is_open()) return false;

    bool loaded = false;
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("currentTerm=", 0) == 0) {
            currentTerm = std::stoi(line.substr(12));
            loaded = true;
        } else if (line.rfind("votedFor=", 0) == 0) {
            votedFor = std::stoi(line.substr(9));
        } else if (line.rfind("log:", 0) == 0) {
            // format: log:index,term,key,value
            std::string s = line.substr(4);
            LogEntry e;
            size_t p0 = 0, p1;
            p1 = s.find(',', p0); e.index = std::stoi(s.substr(p0, p1 - p0)); p0 = p1 + 1;
            p1 = s.find(',', p0); e.term  = std::stoi(s.substr(p0, p1 - p0)); p0 = p1 + 1;
            p1 = s.find(',', p0); e.key   = s[p0];                             p0 = p1 + 1;
            e.value = std::stoi(s.substr(p0));
            // skip entries already covered by the snapshot
            if (e.index > lastIncludedIndex) {
                log.push_back(e);
            }
        }
    }
    return loaded;
}

// save snapshot to snapshot_<nodeId>.dat
void RaftNode::saveSnapshotToFile() const {
    std::string path = "snapshot_" + std::to_string(nodeId) + ".dat";
    std::ofstream f(path);
    if (!f.is_open()) return;
    f << "lastIncludedIndex=" << lastIncludedIndex << "\n";
    f << "lastIncludedTerm="  << lastIncludedTerm  << "\n";
    for (const auto& [k, v] : kvStore.getAll()) {
        f << "kv:" << k << "=" << v << "\n";
    }
}

// load snapshot from snapshot_<nodeId>.dat; returns true if loaded
bool RaftNode::loadSnapshotFromFile() {
    std::string path = "snapshot_" + std::to_string(nodeId) + ".dat";
    std::ifstream f(path);
    if (!f.is_open()) return false;

    int lii = 0, lit = 0;
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("lastIncludedIndex=", 0) == 0)
            lii = std::stoi(line.substr(18));
        else if (line.rfind("lastIncludedTerm=", 0) == 0)
            lit = std::stoi(line.substr(17));
        else if (line.rfind("kv:", 0) == 0) {
            auto eq = line.find('=', 3);
            if (eq != std::string::npos) {
                LogEntry e;
                e.key   = line[3];
                e.value = std::stoi(line.substr(eq + 1));
                e.index = 0;
                e.term  = 0;
                kvStore.apply(e);
            }
        }
    }

    if (lii == 0) return false;

    lastIncludedIndex = lii;
    lastIncludedTerm  = lit;
    log.clear();
    log.push_back({lii, lit, '\0', 0});
    commitIndex = lii;
    lastApplied = lii;
    return true;
}

// handle InstallSnapshot RPC from leader
Message RaftNode::handleInstallSnapshot(const Message& msg) {
    Message reply;
    reply.type     = MessageType::InstallSnapshotReply;
    reply.senderId = nodeId;
    reply.term     = currentTerm;

    if (msg.term < currentTerm) return reply;

    if (msg.term > currentTerm || role != Role::Follower) {
        becomeFollower(msg.term);
    }
    leaderId = msg.leaderId;
    electionTimer.reset();

    // already have this snapshot or a newer one
    if (msg.lastIncludedIndex <= lastIncludedIndex) {
        reply.term = currentTerm;
        return reply;
    }

    // install snapshot: restore state machine from snapshot data
    kvStore = StateMachine{};
    deserializeKVStore(msg.snapshotData);

    lastIncludedIndex = msg.lastIncludedIndex;
    lastIncludedTerm  = msg.lastIncludedTerm;

    // discard entire log; keep only sentinel at snapshot point
    log.clear();
    log.push_back({lastIncludedIndex, lastIncludedTerm, '\0', 0});

    if (commitIndex < lastIncludedIndex) commitIndex = lastIncludedIndex;
    if (lastApplied < lastIncludedIndex) lastApplied = lastIncludedIndex;

    saveSnapshotToFile();
    savePersistentState();  // update persistent log to reflect installed snapshot
    reply.term = currentTerm;
    log_event("Installed snapshot at index=" + std::to_string(lastIncludedIndex));
    return reply;
}

// handle InstallSnapshotReply from a follower
Message RaftNode::handleInstallSnapshotReply(const Message& msg) {
    if (role != Role::Leader) return {};
    if (msg.term > currentTerm) {
        becomeFollower(msg.term);
        return {};
    }
    int peerId = msg.senderId;
    // follower is now caught up to at least lastIncludedIndex
    if (nextIndex[peerId] <= lastIncludedIndex) {
        nextIndex[peerId]  = lastIncludedIndex + 1;
        matchIndex[peerId] = lastIncludedIndex;
    }
    recentAckPeers.insert(peerId);
    checkMajorityAck();
    log_event("Node " + std::to_string(peerId)
              + " installed snapshot up to index=" + std::to_string(matchIndex[peerId]));
    return {};
}

// send a snapshot to a lagging follower
void RaftNode::sendInstallSnapshotToPeer(int peerId) {
    if (role != Role::Leader) return;

    int  lii  = lastIncludedIndex;
    int  lit  = lastIncludedTerm;
    int  term = currentTerm;
    int  lid  = nodeId;
    std::string data = serializeKVStore();
    const auto& peerConfig = config.getNode(peerId);

    std::thread([this, peerConfig, term, lid, lii, lit, data, peerId]() {
        Message req;
        req.type              = MessageType::InstallSnapshot;
        req.term              = term;
        req.senderId          = lid;
        req.leaderId          = lid;
        req.lastIncludedIndex = lii;
        req.lastIncludedTerm  = lit;
        req.snapshotData      = data;

        Message reply;
        if (sendRPC(peerConfig.hostname, peerConfig.port, req, reply)) {
            reply.senderId = peerId;
            eventQueue.push(Event{Event::IncomingRPC, reply, peerId});
        }
    }).detach();
}

// handle client put
Message RaftNode::handleClientPut(const Message& msg) {
    Message reply;
    reply.type = MessageType::ClientResponse;
    reply.senderId = nodeId;

    if (role != Role::Leader) {
        reply.success = false;
        reply.isLeader = false;
        if (leaderId >= 0) {
            const auto& leader = config.getNode(leaderId);
            reply.redirectHost = leader.hostname;
            reply.redirectPort = leader.port;
        }
        return reply;
    }

    LogEntry entry;
    entry.index = lastLogIndex() + 1;
    entry.term = currentTerm;
    entry.key = msg.key;
    entry.value = msg.value;
    log.push_back(entry);
    appendLogEntryToDisk(entry);  // O(1) append; no full rewrite needed

    log_event("Client PUT: " + std::string(1, msg.key) + "="
              + std::to_string(msg.value) + " at index " + std::to_string(entry.index));

    sendHeartbeats();

    reply.success = true;
    reply.isLeader = true;
    return reply;
}

// handle client get
Message RaftNode::handleClientGet(const Message& msg) {
    Message reply;
    reply.type = MessageType::ClientResponse;
    reply.senderId = nodeId;

    if (role != Role::Leader) {
        reply.success = false;
        reply.isLeader = false;
        if (leaderId >= 0) {
            const auto& leader = config.getNode(leaderId);
            reply.redirectHost = leader.hostname;
            reply.redirectPort = leader.port;
        }
        return reply;
    }

    long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    long long staleness = now - lastMajorityAckMs;

    if (lastMajorityAckMs == 0 || staleness > HEARTBEAT_INTERVAL_MS * 2) {
        // Trigger a heartbeat round and tell client to retry
        sendHeartbeats();
        log_event("Client GET: leadership not confirmed, sending heartbeat");
        reply.success = false;
        reply.isLeader = true;
        return reply;
    }

    reply.success = true;
    reply.isLeader = true;
    reply.value = kvStore.get(msg.key);
    return reply;
}

// run the node
void RaftNode::run() {
    running = true;
    const auto& self = config.getNode(nodeId);

    // restore from snapshot first (establishes lastIncludedIndex baseline)
    if (loadSnapshotFromFile()) {
        log_event("Restored from snapshot at index=" + std::to_string(lastIncludedIndex));
    }

    // then restore persistent state (currentTerm, votedFor, and log entries
    // AFTER the snapshot; entries at or before lastIncludedIndex are skipped)
    if (loadPersistentState()) {
        log_event("Restored persistent state: term=" + std::to_string(currentTerm)
                  + " votedFor=" + std::to_string(votedFor)
                  + " logSize=" + std::to_string(log.size()));
    }

    // start the server
    server.start(self.port, [this](const Message& request) -> Message {
        std::lock_guard<std::mutex> lock(raftMtx);

        // handle incoming RPCs
        switch (request.type) {
            case MessageType::RequestVote:
                return handleRequestVote(request);
            case MessageType::AppendEntries:
                return handleAppendEntries(request);
            case MessageType::ClientPut:
                return handleClientPut(request);
            case MessageType::ClientGet:
                return handleClientGet(request);
            case MessageType::InstallSnapshot:
                return handleInstallSnapshot(request);
            default:
                return {};
        }
    });

    log_event("Started on " + self.hostname + ":" + std::to_string(self.port));

    // start election timer
    electionTimer.start(
        [this]() {
            return fixedTimeoutMs > 0 ? fixedTimeoutMs : randomElectionTimeout();
        },
        [this]() { eventQueue.push(Event{Event::ElectionTimeout, {}, -1}); }
    );

    // main event loop
    while (running) {
        Event event = eventQueue.pop();
        std::lock_guard<std::mutex> lock(raftMtx);

        // handle events
        switch (event.type) {
            case Event::ElectionTimeout:
                if (role != Role::Leader) {
                    startElection();
                }
                break;

            case Event::HeartbeatTimeout:
                if (role == Role::Leader) {
                    sendHeartbeats();
                }
                break;

            case Event::IncomingRPC:
                switch (event.msg.type) {
                    case MessageType::RequestVoteReply:
                        handleRequestVoteReply(event.msg);
                        break;
                    case MessageType::AppendEntriesReply:
                        handleAppendEntriesReply(event.msg, event.lastSentIndex);
                        break;
                    case MessageType::InstallSnapshotReply:
                        handleInstallSnapshotReply(event.msg);
                        break;
                    default:
                        break;
                }
                break;
        }

        applyCommitted();
    }
}

// stop the node
void RaftNode::stop() {
    running = false;
    electionTimer.stop();
    heartbeatTimer.stop();
    server.stop();
    // push a dummy event to unblock the main loop
    eventQueue.push(Event{Event::ElectionTimeout, {}, -1});
}
