/*
 * This file is based on Frank J. T. Wojcik's SMHasher3 which itself is based on Thorup's "High Speed Hashing for Integers and Strings" 2018: https://arxiv.org/pdf/1504.06804.pdf
 * Original code: https://gitlab.com/fwojcik/smhasher3/-/blob/main/hashes/multiply_shift.cpp#L113
 *
 */

#pragma once
// Multiply-Vector-Shift: 8 coefficients (uint64_t), 32-bit word lanes, 32 output.
// Single config entry point: set_params(...)
//
// h = sum_i ( uint64_t(w_i) * coeffs[i % 8] ) + (tail * coeffs[len_words % 8])
// return uint32_t(h >> 32)
//
#include <cstdint>
#include <cstddef>
#include <array>
#include <core/unaligned.hpp>

namespace hashfn {

struct MSVec {
  MSVec() { coeffs_.fill(1ull); }

  // Set the 8 coefficients. If force_odd=true (default), force each coeff to be odd.
  void set_params(const std::array<std::uint64_t,8>& coeffs, bool force_odd = true) {
    coeffs_ = coeffs;
    if (force_odd) for (auto& a : coeffs_) a |= 1ull;
  }

  // Hash arbitrary-length byte buffer; returns high 32 bits of 64-bit accumulator.
  inline std::uint32_t hash(const void* in, std::size_t len_bytes) const {
    const std::uint8_t* buf = static_cast<const std::uint8_t*>(in);
    const std::size_t   len = len_bytes / 4;
    std::uint64_t h = 0, t = 0;

    // Full 4-byte words
    for (std::size_t i = 0; i < len; ++i, buf += 4) {
      std::uint32_t w = GET_U32(buf, 0);
      t = static_cast<std::uint64_t>(w) * coeffs_[i & 7u];
      h += t;
    }

    // Tail bytes
    const int remaining = static_cast<int>(len_bytes & 3u);
    if (remaining) {
      std::uint64_t last = 0;
      if (remaining & 2) { last = (last << 16) | GET_U16(buf, 0); buf += 2; }
      if (remaining & 1) { last = (last << 8)  | (*buf); }
      t = last * coeffs_[len & 7u];
      h += t;
    }

    return static_cast<std::uint32_t>(h >> 32);
  }

  const std::array<std::uint64_t,8>& coeffs() const { return coeffs_; }

private:
  std::array<std::uint64_t,8> coeffs_;
};

} // namespace hashfn
