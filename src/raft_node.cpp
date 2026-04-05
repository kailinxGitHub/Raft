#include "raft_node.h"
#include <iostream>
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

// get last log index
int RaftNode::lastLogIndex() const {
    return static_cast<int>(log.size()) - 1;
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

// send append entries to a peer
void RaftNode::sendAppendEntriesToPeer(int peerId) {
    if (role != Role::Leader) return;

    int ni = nextIndex[peerId];
    int prevIdx = ni - 1;
    int prevTrm = (prevIdx >= 0 && prevIdx < static_cast<int>(log.size()))
                  ? log[prevIdx].term : 0;

    // get entries to send
    std::vector<LogEntry> entriesToSend;
    for (int i = ni; i < static_cast<int>(log.size()); i++) {
        entriesToSend.push_back(log[i]);
    }

    // get current term and leader id
    int term = currentTerm;
    int lid = nodeId;
    int lc = commitIndex;
    int lastIdx = static_cast<int>(log.size()) - 1;
    const auto& peerConfig = config.getNode(peerId);

    // send append entries to the peer
    std::thread([this, peerConfig, term, lid, prevIdx, prevTrm, lc,
                 entriesToSend, peerId, lastIdx]() {
        Message req;
        req.type = MessageType::AppendEntries;
        req.term = term;
        req.senderId = lid;
        req.leaderId = lid;
        req.prevLogIndex = prevIdx;
        req.prevLogTerm = prevTrm;
        req.leaderCommit = lc;
        req.entries = entriesToSend;

        Message reply;
        if (sendRPC(peerConfig.hostname, peerConfig.port, req, reply)) {
            reply.senderId = peerId;
            Event e{Event::IncomingRPC, reply, peerId, lastIdx};
            eventQueue.push(e);
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
        if (msg.prevLogIndex >= static_cast<int>(log.size())) {
            log_event("AppendEntries log mismatch: prevLogIndex "
                      + std::to_string(msg.prevLogIndex) + " > log size "
                      + std::to_string(log.size()));
            return reply;
        }
        if (log[msg.prevLogIndex].term != msg.prevLogTerm) {
            log_event("AppendEntries term mismatch at index "
                      + std::to_string(msg.prevLogIndex));
            return reply;
        }
    }

    // delete conflicting entries and append new ones
    int insertIdx = msg.prevLogIndex + 1;
    for (size_t i = 0; i < msg.entries.size(); i++) {
        int logIdx = insertIdx + static_cast<int>(i);
        if (logIdx < static_cast<int>(log.size())) {
            if (log[logIdx].term != msg.entries[i].term) {
                log.resize(logIdx);
                log.push_back(msg.entries[i]);
            }
        } else {
            log.push_back(msg.entries[i]);
        }
    }

    if (msg.leaderCommit > commitIndex) {
        commitIndex = std::min(msg.leaderCommit, static_cast<int>(log.size()) - 1);
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
    for (int n = static_cast<int>(log.size()) - 1; n > commitIndex; n--) {
        if (log[n].term != currentTerm) continue;

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
        kvStore.apply(log[lastApplied]);
        log_event("Applied log[" + std::to_string(lastApplied) + "]: "
                  + log[lastApplied].key + "=" + std::to_string(log[lastApplied].value));
    }
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
    entry.index = static_cast<int>(log.size());
    entry.term = currentTerm;
    entry.key = msg.key;
    entry.value = msg.value;
    log.push_back(entry);

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
