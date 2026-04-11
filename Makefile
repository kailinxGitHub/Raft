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

test_snapshot: $(TESTDIR)/test_snapshot.cpp $(SRCDIR)/network.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

test_stress: $(TESTDIR)/test_stress.cpp $(SRCDIR)/network.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

tests: raft_node test_unit test_integration test_advanced test_edge test_snapshot test_stress

clean:
	rm -f raft_node raft_client test_unit test_integration test_advanced test_edge test_snapshot test_stress test_node_*.log node_*.log snapshot_*.dat raft_state_*.dat

.PHONY: all tests clean
