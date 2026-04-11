#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <sstream>

struct TestResult {
    std::string name;
    bool passed;
    std::string message;
};

static std::vector<TestResult> allResults;
static int totalTests = 0;
static int passedTests = 0;
static int failedTests = 0;

#define ASSERT_TRUE(expr) \
    do { \
        if (!(expr)) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected true: " << #expr; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_FALSE(expr) \
    do { \
        if ((expr)) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected false: " << #expr; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_EQ(a, b) \
    do { \
        auto _a = (a); auto _b = (b); \
        if (_a != _b) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected " << #a << " == " << #b \
                << ", got " << _a << " vs " << _b; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_NE(a, b) \
    do { \
        auto _a = (a); auto _b = (b); \
        if (_a == _b) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected " << #a << " != " << #b \
                << ", got " << _a; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_GE(a, b) \
    do { \
        auto _a = (a); auto _b = (b); \
        if (_a < _b) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected " << #a << " >= " << #b \
                << ", got " << _a << " < " << _b; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_LE(a, b) \
    do { \
        auto _a = (a); auto _b = (b); \
        if (_a > _b) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected " << #a << " <= " << #b \
                << ", got " << _a << " > " << _b; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_GT(a, b) \
    do { \
        auto _a = (a); auto _b = (b); \
        if (_a <= _b) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected " << #a << " > " << #b \
                << ", got " << _a << " <= " << _b; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_THROWS(expr) \
    do { \
        bool threw = false; \
        try { expr; } catch (...) { threw = true; } \
        if (!threw) { \
            std::ostringstream oss; \
            oss << "  FAIL at " << __FILE__ << ":" << __LINE__ \
                << " — expected exception from: " << #expr; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define RUN_TEST(testFunc) \
    do { \
        totalTests++; \
        std::cout << "  Running " << #testFunc << "..."; \
        try { \
            testFunc(); \
            std::cout << " PASS" << std::endl; \
            passedTests++; \
            allResults.push_back({#testFunc, true, ""}); \
        } catch (const std::exception& e) { \
            std::cout << " FAIL" << std::endl; \
            std::cerr << e.what() << std::endl; \
            failedTests++; \
            allResults.push_back({#testFunc, false, e.what()}); \
        } \
    } while (0)

#define TEST_SECTION(name) \
    std::cout << "\n=== " << name << " ===" << std::endl

inline int printSummary() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "Results: " << passedTests << "/" << totalTests << " passed";
    if (failedTests > 0) {
        std::cout << " (" << failedTests << " failed)";
    }
    std::cout << std::endl;

    if (failedTests > 0) {
        std::cout << "\nFailed tests:" << std::endl;
        for (const auto& r : allResults) {
            if (!r.passed) {
                std::cout << "  - " << r.name << std::endl;
                std::cout << "    " << r.message << std::endl;
            }
        }
    }

    std::cout << "========================================" << std::endl;
    return failedTests > 0 ? 1 : 0;
}
