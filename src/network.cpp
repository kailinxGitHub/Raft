#include "network.h"
#include "config.h"
#include <sstream>
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>

// helper to convert message type to string
static std::string messageTypeToString(MessageType t) {
    switch (t) {
        case MessageType::RequestVote:        return "RequestVote";
        case MessageType::RequestVoteReply:   return "RequestVoteReply";
        case MessageType::AppendEntries:      return "AppendEntries";
        case MessageType::AppendEntriesReply: return "AppendEntriesReply";
        case MessageType::ClientPut:             return "ClientPut";
        case MessageType::ClientGet:             return "ClientGet";
        case MessageType::ClientResponse:        return "ClientResponse";
        case MessageType::InstallSnapshot:       return "InstallSnapshot";
        case MessageType::InstallSnapshotReply:  return "InstallSnapshotReply";
    }
    return "Unknown";
}

// helper to convert string to message type
static MessageType stringToMessageType(const std::string& s) {
    if (s == "RequestVote")        return MessageType::RequestVote;
    if (s == "RequestVoteReply")   return MessageType::RequestVoteReply;
    if (s == "AppendEntries")      return MessageType::AppendEntries;
    if (s == "AppendEntriesReply") return MessageType::AppendEntriesReply;
    if (s == "ClientPut")             return MessageType::ClientPut;
    if (s == "ClientGet")             return MessageType::ClientGet;
    if (s == "ClientResponse")        return MessageType::ClientResponse;
    if (s == "InstallSnapshot")       return MessageType::InstallSnapshot;
    if (s == "InstallSnapshotReply")  return MessageType::InstallSnapshotReply;
    throw std::runtime_error("Unknown message type: " + s);
}

// helper to serialize log entries
static std::string serializeEntries(const std::vector<LogEntry>& entries) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < entries.size(); i++) {
        if (i > 0) oss << ";";
        oss << entries[i].index << "," << entries[i].term << ","
            << entries[i].key << "," << entries[i].value;
    }
    oss << "]";
    return oss.str();
}

// helper to deserialize log entries
static std::vector<LogEntry> deserializeEntries(const std::string& s) {
    std::vector<LogEntry> entries;
    if (s.size() <= 2 || s == "[]") return entries;
    std::string inner = s.substr(1, s.size() - 2);
    std::istringstream iss(inner);
    std::string token;
    while (std::getline(iss, token, ';')) {
        LogEntry e;
        char comma;
        std::istringstream tiss(token);
        tiss >> e.index >> comma >> e.term >> comma >> e.key >> comma >> e.value;
        entries.push_back(e);
    }
    return entries;
}

// serialize message
std::string serialize(const Message& msg) {
    std::ostringstream oss;
    oss << "type=" << messageTypeToString(msg.type)
        << "|term=" << msg.term
        << "|senderId=" << msg.senderId;

    switch (msg.type) {
        case MessageType::RequestVote:
            oss << "|candidateId=" << msg.candidateId
                << "|lastLogIndex=" << msg.lastLogIndex
                << "|lastLogTerm=" << msg.lastLogTerm;
            break;
        case MessageType::RequestVoteReply:
            oss << "|voteGranted=" << (msg.voteGranted ? "true" : "false");
            break;
        case MessageType::AppendEntries:
            oss << "|leaderId=" << msg.leaderId
                << "|prevLogIndex=" << msg.prevLogIndex
                << "|prevLogTerm=" << msg.prevLogTerm
                << "|leaderCommit=" << msg.leaderCommit
                << "|entries=" << serializeEntries(msg.entries);
            break;
        case MessageType::AppendEntriesReply:
            oss << "|success=" << (msg.success ? "true" : "false");
            break;
        case MessageType::ClientPut:
            oss << "|key=" << msg.key
                << "|value=" << msg.value;
            break;
        case MessageType::ClientGet:
            oss << "|key=" << msg.key;
            break;
        case MessageType::ClientResponse:
            oss << "|success=" << (msg.success ? "true" : "false")
                << "|value=" << msg.value
                << "|isLeader=" << (msg.isLeader ? "true" : "false")
                << "|redirectHost=" << msg.redirectHost
                << "|redirectPort=" << msg.redirectPort;
            break;
        case MessageType::InstallSnapshot:
            oss << "|leaderId=" << msg.leaderId
                << "|lastIncludedIndex=" << msg.lastIncludedIndex
                << "|lastIncludedTerm=" << msg.lastIncludedTerm
                << "|snapshotData=" << msg.snapshotData;
            break;
        case MessageType::InstallSnapshotReply:
            // only term and senderId (already written above)
            break;
    }
    return oss.str();
}

// deserialize message
Message deserialize(const std::string& line) {
    Message msg;
    std::istringstream iss(line);
    std::string pair;
    std::string entriesStr;

    while (std::getline(iss, pair, '|')) {
        auto eq = pair.find('=');
        if (eq == std::string::npos) continue;
        std::string key = pair.substr(0, eq);
        std::string val = pair.substr(eq + 1);

        if (key == "type")           msg.type = stringToMessageType(val);
        else if (key == "term")      msg.term = std::stoi(val);
        else if (key == "senderId")  msg.senderId = std::stoi(val);
        else if (key == "candidateId") msg.candidateId = std::stoi(val);
        else if (key == "lastLogIndex") msg.lastLogIndex = std::stoi(val);
        else if (key == "lastLogTerm")  msg.lastLogTerm = std::stoi(val);
        else if (key == "voteGranted")  msg.voteGranted = (val == "true");
        else if (key == "leaderId")     msg.leaderId = std::stoi(val);
        else if (key == "prevLogIndex") msg.prevLogIndex = std::stoi(val);
        else if (key == "prevLogTerm")  msg.prevLogTerm = std::stoi(val);
        else if (key == "leaderCommit") msg.leaderCommit = std::stoi(val);
        else if (key == "entries")      entriesStr = val;
        else if (key == "success")      msg.success = (val == "true");
        else if (key == "key")          msg.key = val.empty() ? '\0' : val[0];
        else if (key == "value")        msg.value = std::stoi(val);
        else if (key == "isLeader")     msg.isLeader = (val == "true");
        else if (key == "redirectHost")      msg.redirectHost = val;
        else if (key == "redirectPort")      msg.redirectPort = std::stoi(val);
        else if (key == "lastIncludedIndex") msg.lastIncludedIndex = std::stoi(val);
        else if (key == "lastIncludedTerm")  msg.lastIncludedTerm  = std::stoi(val);
        else if (key == "snapshotData")      msg.snapshotData = val;
    }

    if (!entriesStr.empty()) {
        msg.entries = deserializeEntries(entriesStr);
    }
    return msg;
}

// helper to set socket timeout
static void setSocketTimeout(int fd, int timeoutMs) {
    struct timeval tv;
    tv.tv_sec = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

// helper to read line from socket
static std::string readLine(int fd) {
    std::string result;
    char c;
    while (true) {
        ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) break;
        if (c == '\n') break;
        result += c;
    }
    return result;
}

// helper to write line to socket
static bool writeLine(int fd, const std::string& line) {
    std::string data = line + "\n";
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.c_str() + sent, data.size() - sent, 0);
        if (n <= 0) return false;
        sent += n;
    }
    return true;
}

// send RPC to a host and port
bool sendRPC(const std::string& host, int port, const Message& request, Message& reply) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    setSocketTimeout(fd, SOCKET_TIMEOUT_MS);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        close(fd);
        return false;
    }
    addr.sin_port = htons(port);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    if (!writeLine(fd, serialize(request))) {
        close(fd);
        return false;
    }

    std::string replyLine = readLine(fd);
    close(fd);

    if (replyLine.empty()) return false;
    reply = deserialize(replyLine);
    return true;
}

// start the server
void TCPServer::start(int port, Handler handler) {
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listenFd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(listenFd);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port));
    }

    if (listen(listenFd, 16) < 0) {
        close(listenFd);
        throw std::runtime_error("Failed to listen");
    }

    running = true;

    std::thread([this, handler]() {
        while (running) {
            struct sockaddr_in clientAddr;
            socklen_t clientLen = sizeof(clientAddr);
            int clientFd = accept(listenFd, (struct sockaddr*)&clientAddr, &clientLen);
            if (clientFd < 0) continue;

            std::thread([clientFd, handler]() {
                setSocketTimeout(clientFd, SOCKET_TIMEOUT_MS);
                std::string line = readLine(clientFd);
                if (!line.empty()) {
                    try {
                        Message request = deserialize(line);
                        Message reply = handler(request);
                        writeLine(clientFd, serialize(reply));
                    } catch (...) {}
                }
                close(clientFd);
            }).detach();
        }
    }).detach();
}

// stop the server
void TCPServer::stop() {
    running = false;
    if (listenFd >= 0) {
        close(listenFd);
        listenFd = -1;
    }
}

// destructor
TCPServer::~TCPServer() {
    stop();
}
