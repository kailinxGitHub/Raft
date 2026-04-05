#pragma once
#include <string>
#include <sstream>

// log entry struct
struct LogEntry {
    int index;
    int term;
    char key;
    int value;

    // convert log entry to string
    std::string toString() const {
        std::ostringstream oss;
        oss << "(" << index << "," << term << "," << key << "," << value << ")";
        return oss.str();
    }

    // dummy log entry
    static LogEntry dummy() {
        return LogEntry{0, 0, '\0', 0};
    }
};
