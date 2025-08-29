#pragma once
// A1MixedStream:
// - Emits exactly N items.
// - First half (t in [0, floor(N/2)-1]): unique integers 1..floor(N/2).
// - Second half (t in [floor(N/2), N-1]): A1-skewed sequence where key i
//   repeats w(i) = ceil(i/100) times before moving to i+1.
//
// A1MixedSplitStream:
// - Deterministic 50/50 split of the same N positions using splitmix64(seed + t) & 1.
// - group_id = 0 or 1 decides which occurrences this stream emits.
//
#include <span>
#include <cstddef>
#include <cstdint>
#include <array>
#include <algorithm>
#include <type_traits>
#include <core/dataset.hpp>

namespace datasets {

struct A1MixedStream : public Stream {
  explicit A1MixedStream(std::size_t total_items)
    : total_(total_items), half_(total_items / 2) { reset(); }

  bool next(std::span<const std::byte>& out_key) override {
    if (emitted_ >= total_) return false;

    if (emitted_ < half_) {
      // Unique half: value = (emitted_ + 1)
      const uint32_t v = static_cast<uint32_t>(emitted_ + 1);
      valbuf_ = to_bytes(v);
    } else {
      // Skew half: A1 sequence starting at i=1 with repeats ceil(i/100)
      valbuf_ = to_bytes(cur_key_);
    }

    out_key = std::span<const std::byte>(valbuf_.data(), valbuf_.size());

    // Advance global position
    ++emitted_;
    if (emitted_ > half_) { // we're inside the skew half (strictly after the boundary)
      ++cur_rep_;
      if (cur_rep_ >= repeats(cur_key_)) {
        cur_rep_ = 0;
        ++cur_key_;
      }
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
  std::size_t half_;
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

struct A1MixedSplitStream : public Stream {
  A1MixedSplitStream(std::size_t total_items, std::uint64_t seed, int group_id)
    : total_(total_items), half_(total_items/2), seed_(seed), group_(group_id) { reset(); }

  bool next(std::span<const std::byte>& out_key) override {
    while (emitted_ < total_) {
      // Prepare value for current global index 'emitted_'
      if (emitted_ < half_) {
        const uint32_t v = static_cast<uint32_t>(emitted_ + 1);
        valbuf_ = to_bytes(v);
      } else {
        valbuf_ = to_bytes(cur_key_);
      }

      // Decide split group for this index (deterministic 0/1)
      const int g = int(splitmix64(seed_ + emitted_) & 1ull);

      // Advance the underlying base sequence (always)
      ++emitted_;
      if (emitted_ > half_) {
        ++cur_rep_;
        if (cur_rep_ >= A1MixedStream::repeats(cur_key_)) {
          cur_rep_ = 0;
          ++cur_key_;
        }
      }

      // Emit only if this occurrence belongs to 'group_'
      if (g == group_) {
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
  std::size_t half_;
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

} // namespace datasets
