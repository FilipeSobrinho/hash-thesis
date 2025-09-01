#pragma once
// A1Mixed materialized datasets (ptr,len API, fully pre-generated):
// - A1MixedMaterialized: emits exactly N items where the first half are unique
//   integers 1..floor(N/2) (each encoded as 4B little-endian), and the second half
//   follows the A1 skew rule: repeats w(i)=ceil(i/100) for i=1,2,...
// - A1MixedSplitMaterialized: builds two fully materialized 50/50 random splits
//   (group 0 and group 1) using splitmix64(seed + position) & 1 on the global index.
//
// Streaming is zero-overhead: each Stream is just an index over a contiguous byte buffer.
// NOTE: keys are fixed-size 4 bytes (little-endian).
#include <cstddef>
#include <cstdint>
#include <vector>
#include <array>
#include <algorithm>
#include "dataset.hpp"

namespace datasets {

// Simple view/stream over a contiguous buffer of fixed-size keys (4 bytes each).
class BufferStream final : public Stream {
public:
  BufferStream() = default;
  BufferStream(const uint8_t* base, std::size_t count)
    : base_(base), count_(count) {}
  bool next(const void*& out_ptr, std::size_t& out_len) override {
    if (idx_ >= count_) return false;
    out_ptr = base_ + (idx_ * 4);
    out_len = 4;
    ++idx_;
    return true;
  }
  void reset() override { idx_ = 0; }
private:
  const uint8_t* base_{nullptr};
  std::size_t count_{0};
  std::size_t idx_{0};
};

// Helper: ceil(i/100) for repetition count.
static inline uint32_t a1_repeats(uint32_t i) {
  return (i + 99u) / 100u;
}

// Writes a 32-bit value into 4 little-endian bytes at dst[0..3].
static inline void store_le_u32(uint8_t dst[4], uint32_t v) {
  dst[0] = static_cast<uint8_t>(v & 0xFF);
  dst[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
  dst[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
  dst[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
}

// Stateless PRNG used for splitting.
static inline uint64_t splitmix64(uint64_t x){
  x += 0x9E3779B97F4A7C15ull;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
  x = x ^ (x >> 31);
  return x;
}

struct A1MixedMaterialized {
  explicit A1MixedMaterialized(std::size_t N) : N_(N) {
    buf_.resize(N_ * 4); // 4 bytes per key
    // Fill first half with unique values 1..floor(N/2)
    const std::size_t half = N_ / 2;
    for (std::size_t i = 0; i < half; ++i) {
      store_le_u32(&buf_[i*4], static_cast<uint32_t>(i + 1));
    }
    // Fill second half with A1 skew
    uint32_t key = 1, rep = 0;
    for (std::size_t pos = half; pos < N_; ++pos) {
      store_le_u32(&buf_[pos*4], key);
      ++rep;
      if (rep >= a1_repeats(key)) { rep = 0; ++key; }
    }
  }

  std::size_t size() const { return N_; }
  const uint8_t* data() const { return buf_.data(); }
  const std::vector<uint8_t>& bytes() const { return buf_; }

  BufferStream make_stream() const { return BufferStream(buf_.data(), N_); }

private:
  std::size_t N_;
  std::vector<uint8_t> buf_;
};

struct A1MixedSplitMaterialized {
  // Build both groups fully in memory.
  A1MixedSplitMaterialized(std::size_t N, uint64_t seed) {
    const std::size_t half = N / 2;
    bufA_.reserve(((N + 1) / 2) * 4);
    bufB_.reserve(((N + 1) / 2) * 4);

    uint32_t key = 1, rep = 0;
    for (std::size_t pos = 0; pos < N; ++pos) {
      uint32_t v;
      if (pos < half) v = static_cast<uint32_t>(pos + 1);
      else {
        v = key;
        ++rep; if (rep >= a1_repeats(key)) { rep = 0; ++key; }
      }
      // Decide group by global position 'pos'
      const int g = int(splitmix64(seed + pos) & 1ull);

      uint8_t bytes[4];
      store_le_u32(bytes, v);
      if (g == 0) {
        bufA_.insert(bufA_.end(), bytes, bytes+4);
      } else {
        bufB_.insert(bufB_.end(), bytes, bytes+4);
      }
    }
  }

  std::size_t sizeA() const { return bufA_.size() / 4; }
  std::size_t sizeB() const { return bufB_.size() / 4; }
  const uint8_t* dataA() const { return bufA_.data(); }
  const uint8_t* dataB() const { return bufB_.data(); }
  const std::vector<uint8_t>& bytesA() const { return bufA_; }
  const std::vector<uint8_t>& bytesB() const { return bufB_; }

  BufferStream make_streamA() const { return BufferStream(bufA_.data(), sizeA()); }
  BufferStream make_streamB() const { return BufferStream(bufB_.data(), sizeB()); }

private:
  std::vector<uint8_t> bufA_;
  std::vector<uint8_t> bufB_;
};

} // namespace datasets
