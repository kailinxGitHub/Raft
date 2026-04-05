CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pthread -g

SRCDIR = src
TESTDIR = tests
NODE_SRCS = $(SRCDIR)/main.cpp $(SRCDIR)/raft_node.cpp $(SRCDIR)/network.cpp $(SRCDIR)/timer.cpp
CLIENT_SRCS = $(SRCDIR)/client.cpp $(SRCDIR)/network.cpp

all: raft_node raft_client

raft_node: $(NODE_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

raft_client: $(CLIENT_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

test_unit: $(TESTDIR)/test_unit.cpp $(SRCDIR)/network.cpp $(SRCDIR)/timer.cpp $(SRCDIR)/raft_node.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

test_integration: $(TESTDIR)/test_integration.cpp $(SRCDIR)/network.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

test_advanced: $(TESTDIR)/test_advanced.cpp $(SRCDIR)/network.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

test_edge: $(TESTDIR)/test_edge_cases.cpp $(SRCDIR)/network.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

tests: test_unit test_integration test_advanced test_edge

clean:
	rm -f raft_node raft_client test_unit test_integration test_advanced test_edge test_node_*.log node_*.log

.PHONY: all tests clean
