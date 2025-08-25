#pragma once
#include <cstdint>
#include <cstddef>
#include <span>

struct IHash {
  virtual ~IHash() = default;
  virtual uint64_t operator()(std::span<const std::byte> key) = 0;
  virtual void reseed(uint64_t s0, uint64_t s1 = 0) = 0;
};
