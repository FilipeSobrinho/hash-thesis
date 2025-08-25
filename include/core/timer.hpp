#pragma once
#include <chrono>

struct ScopedTimer {
  using clock = std::chrono::steady_clock;
  clock::time_point t0;
  double& out;
  explicit ScopedTimer(double& seconds) : t0(clock::now()), out(seconds) {}
  ~ScopedTimer() { out = std::chrono::duration<double>(clock::now() - t0).count(); }
};
