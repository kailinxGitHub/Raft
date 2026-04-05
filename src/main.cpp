#include "raft_node.h"
#include <iostream>
#include <csignal>
#include <cstdlib>

// global node pointer to node instance
static RaftNode* globalNode = nullptr;

// signal handler to stop node
static void signalHandler(int) {
    if (globalNode) globalNode->stop();
}

// main function to start node
int main(int argc, char* argv[]) {
    // check if arguments are valid
    if (argc < 3 || argc > 4) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <cluster.conf> [--fixed-timeout <ms>]\n";
        return 1;
    }

    // parse arguments
    int nodeId = std::atoi(argv[1]);
    std::string configPath = argv[2];
    int fixedTimeout = 0;
    if (argc == 4) fixedTimeout = std::atoi(argv[3]);

    // load cluster configuration
    ClusterConfig config = ClusterConfig::load(configPath);

    // create node instance
    RaftNode node(nodeId, config, fixedTimeout);
    // set global node pointer
    globalNode = &node;

    // register signal handlers
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    // run node
    node.run();
    return 0;
}
