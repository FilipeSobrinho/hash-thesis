#pragma once
// A1MixedStream (ptr,len API):
// - Emits exactly N items.
// - First half: unique integers 1..floor(N/2), each encoded as 4 little-endian bytes.
// - Second half: A1-skewed sequence with repeats w(i)=ceil(i/100), starting at key=1,
//   also encoded as 4 little-endian bytes.
#include <cstddef>
#include <cstdint>
#include <array>
#include "dataset.hpp"

namespace datasets {

struct A1MixedStream : public Stream {
  explicit A1MixedStream(std::size_t total_items)
    : total_(total_items), half_(total_items / 2) { reset(); }

  bool next(const void*& out_ptr, std::size_t& out_len) override {
    if (emitted_ >= total_) return false;

    if (emitted_ < half_) {
      const uint32_t v = static_cast<uint32_t>(emitted_ + 1);
      store_le_u32(v);
    } else {
      store_le_u32(cur_key_);
    }

    out_ptr = valbuf_.data();
    out_len = valbuf_.size();

    // Advance position
    ++emitted_;
    if (emitted_ > half_) {
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

  static inline uint32_t repeats(uint32_t key) {
    return (key + 99u) / 100u; // ceil(key/100)
  }

private:
  std::size_t total_;
  std::size_t half_;
  std::size_t emitted_{0};
  uint32_t    cur_key_{1};
  uint32_t    cur_rep_{0};
  std::array<uint8_t,4> valbuf_{};  // byte buffer for current key

  inline void store_le_u32(uint32_t v){
    valbuf_[0] = static_cast<uint8_t>(v & 0xFF);
    valbuf_[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
    valbuf_[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
    valbuf_[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
  }
};

} // namespace datasets
