#pragma once
#include <span>
#include <cstddef>
#include <vector>
#include <cstdint>

struct Stream {
  virtual ~Stream() = default;
  // Writes a view over the next key into out_key; returns false when exhausted.
  virtual bool next(std::span<const std::byte>& out_key) = 0;
  virtual void reset() = 0;
};
