#pragma once
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

// timer class
class Timer {
    std::thread thread;
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> running{false};
    std::atomic<bool> stopped{false};
    bool resetFlag = false;
    std::function<int()> intervalFn;
    std::function<void()> callback;

public:
    // start the timer
    void start(std::function<int()> intervalFn, std::function<void()> callback);
    // reset the timer
    void reset();
    // stop the timer
    void stop();
    // destructor
    ~Timer();
};
