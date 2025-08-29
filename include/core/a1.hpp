#pragma once
// A1: stream exactly N items where key i appears
// w(i) = ceil(i/100) times before advancing to i+1.
// Keys are 32-bit little-endian integers starting at i=1.
//
// Also provides a deterministic 50/50 split variant for OPH-style tests.
//
#include <span>
#include <cstddef>
#include <cstdint>
#include <array>
#include "dataset.hpp"

struct A1TotalStream : public Stream {
  explicit A1TotalStream(std::size_t total_items)
    : total_(total_items) { reset(); }

  bool next(std::span<const std::byte>& out_key) override {
    if (emitted_ >= total_) return false;
    valbuf_ = to_bytes(cur_key_);
    out_key = std::span<const std::byte>(valbuf_.data(), valbuf_.size());
    ++emitted_;
    // advance repetition/key counters
    ++cur_rep_;
    if (cur_rep_ >= repeats(cur_key_)) {
      cur_rep_ = 0;
      ++cur_key_;
    }
    return true;
  }

  void reset() override {
    emitted_ = 0;
    cur_key_ = 1;
    cur_rep_ = 0;
  }

  std::size_t size_hint() const { return total_; }
  static inline std::uint32_t repeats(std::uint32_t key) {
    return (key + 99u) / 100u; // ceil(key/100)
  }

private:
  std::size_t total_;
  std::size_t emitted_{0};
  std::uint32_t cur_key_{1};
  std::uint32_t cur_rep_{0};
  std::array<std::byte,4> valbuf_{};
  static std::array<std::byte,4> to_bytes(std::uint32_t v){
    std::array<std::byte,4> b{};
    b[0] = std::byte(std::uint8_t(v & 0xFF));
    b[1] = std::byte(std::uint8_t((v >> 8) & 0xFF));
    b[2] = std::byte(std::uint8_t((v >> 16) & 0xFF));
    b[3] = std::byte(std::uint8_t((v >> 24) & 0xFF));
    return b;
  }
};

// -------------- 50/50 split variant (no max-key) ----------------
struct A1TotalSplitStream : public Stream {
  A1TotalSplitStream(std::size_t total_items, std::uint64_t seed, int group_id)
    : total_(total_items), seed_(seed), group_(group_id) { reset(); }

  bool next(std::span<const std::byte>& out_key) override {
    while (emitted_ < total_) {
      // Decide group for this global index
      std::uint64_t g = splitmix64(seed_ + emitted_) & 1ull;
      // Prepare key bytes for current position
      valbuf_ = to_bytes(cur_key_);
      // Advance base sequence (always)
      ++emitted_;
      ++cur_rep_;
      if (cur_rep_ >= A1TotalStream::repeats(cur_key_)) {
        cur_rep_ = 0;
        ++cur_key_;
      }
      if (int(g) == group_) {
        out_key = std::span<const std::byte>(valbuf_.data(), valbuf_.size());
        return true;
      }
    }
    return false;
  }

  void reset() override {
    emitted_ = 0;
    cur_key_ = 1;
    cur_rep_ = 0;
  }

  std::size_t size_hint() const { return total_; }

private:
  std::size_t total_;
  std::uint64_t seed_;
  int group_;
  std::size_t emitted_{0};
  std::uint32_t cur_key_{1};
  std::uint32_t cur_rep_{0};
  std::array<std::byte,4> valbuf_{};

  static std::array<std::byte,4> to_bytes(std::uint32_t v){
    std::array<std::byte,4> b{};
    b[0] = std::byte(std::uint8_t(v & 0xFF));
    b[1] = std::byte(std::uint8_t((v >> 8) & 0xFF));
    b[2] = std::byte(std::uint8_t((v >> 16) & 0xFF));
    b[3] = std::byte(std::uint8_t((v >> 24) & 0xFF));
    return b;
  }
  static inline std::uint64_t splitmix64(std::uint64_t x){
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    x = x ^ (x >> 31);
    return x;
  }
};
