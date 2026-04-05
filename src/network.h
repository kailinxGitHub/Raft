#pragma once
#include <string>
#include <vector>
#include <functional>
#include <atomic>
#include "log_entry.h"

// message type enum
enum class MessageType {
    RequestVote,
    RequestVoteReply,
    AppendEntries,
    AppendEntriesReply,
    ClientPut,
    ClientGet,
    ClientResponse
};

// message struct
struct Message {
    MessageType type;
    int term = 0;
    int senderId = -1;

    // request vote fields
    int candidateId = -1;
    int lastLogIndex = 0;
    int lastLogTerm = 0;

    // append entries fields
    int leaderId = -1;
    int prevLogIndex = 0;
    int prevLogTerm = 0;
    int leaderCommit = 0;
    std::vector<LogEntry> entries;

    // reply fields
    bool voteGranted = false;
    bool success = false;

    // client fields
    char key = '\0';
    int value = 0;
    std::string redirectHost;
    int redirectPort = 0;
    bool isLeader = false;
};
// serialize message
std::string serialize(const Message& msg);
// deserialize message
Message deserialize(const std::string& line);

// send RPC to a host and port
bool sendRPC(const std::string& host, int port, const Message& request, Message& reply);

// TCP server class
class TCPServer {
    int listenFd = -1;
    std::atomic<bool> running{false};

public:
    using Handler = std::function<Message(const Message&)>;

    // start the server
    void start(int port, Handler handler);
    // stop the server
    void stop();
    ~TCPServer();
};
