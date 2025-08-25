#pragma once
#include <cstddef>
#include <span>

struct ISketch {
  virtual ~ISketch() = default;
  virtual void update(std::span<const std::byte> key, uint64_t w = 1) = 0;
};
