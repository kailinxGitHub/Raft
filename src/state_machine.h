#pragma once
#include <map>
#include "log_entry.h"

// state machine class
class StateMachine {
    std::map<char, int> store;

public:
    // apply log entry to state machine
    void apply(const LogEntry& entry) {
        if (entry.key != '\0') {
            store[entry.key] = entry.value;
        }
    }

    // get value by key
    int get(char key) const {
        auto it = store.find(key);
        return (it != store.end()) ? it->second : -1;
    }

    // get all key-value pairs
    const std::map<char, int>& getAll() const { return store; }
};
