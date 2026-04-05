#include "network.h"
#include "config.h"
#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>
#include <chrono>

// main function to start client
int main(int argc, char* argv[]) {
    // validity
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <cluster.conf> <node_id>\n"
                  << "Commands:\n"
                  << "  put <key> <value>  - Store a key-value pair\n"
                  << "  get <key>          - Retrieve a value\n"
                  << "  quit               - Exit\n";
        return 1;
    }

    // cluster config
    ClusterConfig config = ClusterConfig::load(argv[1]);
    // target node id
    int targetId = std::atoi(argv[2]);

    // host and port
    std::string host = config.getNode(targetId).hostname;
    int port = config.getNode(targetId).port;

    // connected to node
    std::cout << "Connected to Node " << targetId
              << " at " << host << ":" << port << "\n";

    // main loop
    std::string line;
    while (std::cout << "> " && std::getline(std::cin, line)) {
        // quit or exit
        if (line == "quit" || line == "exit") break;
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        Message req;
        req.senderId = -1;

        // put or get
        if (cmd == "put") {
            char key;
            int value;
            if (!(iss >> key >> value)) {
                std::cout << "Usage: put <key> <value>\n";
                continue;
            }
            req.type = MessageType::ClientPut;
            req.key = key;
            req.value = value;
        } else if (cmd == "get") {
            char key;
            if (!(iss >> key)) {
                std::cout << "Usage: get <key>\n";
                continue;
            }
            req.type = MessageType::ClientGet;
            req.key = key;
        } else {
            std::cout << "Unknown command: " << cmd << "\n";
            continue;
        }

        // send RPC
        Message reply;
        if (!sendRPC(host, port, req, reply)) {
            std::cout << "Error: could not reach Node " << targetId << "\n";
            continue;
        }

        // update target if not leader
        if (!reply.isLeader) {
            std::cout << "Node " << targetId << " is not the leader.";
            if (!reply.redirectHost.empty() && reply.redirectPort > 0) {
                host = reply.redirectHost;
                port = reply.redirectPort;
                for (const auto& n : config.nodes) {
                    if (n.hostname == host && n.port == port) {
                        targetId = n.id;
                        break;
                    }
                }
                std::cout << " Redirected to Node " << targetId
                          << " at " << host << ":" << port;
            }
            std::cout << "\n";
            continue;
        }

        // print result
        if (cmd == "put") {
            std::cout << (reply.success ? "OK" : "FAIL") << "\n";
        } else if (cmd == "get") {
            // retry
            if (!reply.success && reply.isLeader) {
                for (int retry = 0; retry < 3 && !reply.success; retry++) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(150));
                    if (!sendRPC(host, port, req, reply)) break;
                }
            }
            if (reply.success) {
                std::cout << reply.value << "\n";
            } else {
                std::cout << "FAIL\n";
            }
        }
    }

    return 0;
}
