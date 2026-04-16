#include "timer.h"

// start the timer
void Timer::start(std::function<int()> intervalFunc, std::function<void()> cb) {
    stop();
    intervalFn = std::move(intervalFunc);
    callback = std::move(cb);
    stopped = false;
    running = true;

    thread = std::thread([this]() {
        while (running) {
            int ms = intervalFn();
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait_for(lock, std::chrono::milliseconds(ms),
                [this]() { return stopped.load() || resetFlag; });
            if (stopped) break;
            if (resetFlag) {
                resetFlag = false;
                continue;
            }
            if (running) {
                callback();
            }
        }
    });
}

// reset the timer
void Timer::reset() {
    std::lock_guard<std::mutex> lock(mtx);
    resetFlag = true;
    cv.notify_all();
}

// stop the timer
void Timer::stop() {
    running = false;
    stopped = true;
    cv.notify_all();
    if (thread.joinable()) {
        thread.join();
    }
}

// destructor
Timer::~Timer() {
    stop();
}
