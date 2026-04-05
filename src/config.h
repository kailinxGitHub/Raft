#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <random>
#include <chrono>

constexpr int ELECTION_TIMEOUT_MIN_MS = 300;
constexpr int ELECTION_TIMEOUT_MAX_MS = 600;
constexpr int HEARTBEAT_INTERVAL_MS = 100;
constexpr int SOCKET_TIMEOUT_MS = 500;

// node config struct
struct NodeConfig {
    int id;
    std::string hostname;
    int port;
};

// cluster config struct
struct ClusterConfig {
    std::vector<NodeConfig> nodes;

    // load cluster config from file
    static ClusterConfig load(const std::string& path) {
        ClusterConfig config;
        std::ifstream file(path);
        if (!file.is_open()) {
            throw std::runtime_error("Cannot open cluster config: " + path);
        }
        std::string line;
        while (std::getline(file, line)) {
            if (line.empty() || line[0] == '#') continue;
            std::istringstream iss(line);
            NodeConfig node;
            if (iss >> node.id >> node.hostname >> node.port) {
                config.nodes.push_back(node);
            }
        }
        return config;
    }

    // get node config by id
    const NodeConfig& getNode(int id) const {
        for (const auto& node : nodes) {
            if (node.id == id) return node;
        }
        throw std::runtime_error("Node not found: " + std::to_string(id));
    }

    int clusterSize() const { return static_cast<int>(nodes.size()); }
    int majority() const { return clusterSize() / 2 + 1; }
};

// generate random election timeout
inline int randomElectionTimeout() {
    static thread_local std::mt19937 gen(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<int> dist(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
    return dist(gen);
}
