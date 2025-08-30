#pragma once
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <core/unaligned.hpp>

using seed_t = uint64_t;

// Simple demo multiply-shift that consumes up to 4 bytes (pads if shorter).
// Writes a 32-bit result to *out.
static inline void ms32_demo(const void* in, const size_t len_bytes, const seed_t seed, void* out) {
  const uint8_t* buf = static_cast<const uint8_t*>(in);
  uint32_t x = 0;
  if (len_bytes >= 4) {
    x = GET_U32(buf, 0);
  } else if (len_bytes == 3) {
    x = (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) | ((uint32_t)buf[2] << 16);
  } else if (len_bytes == 2) {
    x = GET_U16(buf, 0);
  } else if (len_bytes == 1) {
    x = buf[0];
  } // else x=0
  // Derive a,b from seed (very simple; replace with your own if needed)
  uint32_t a = (uint32_t)(seed | 1u);      // make odd
  uint32_t b = (uint32_t)(seed >> 32);
  uint32_t y = (uint32_t)((uint64_t)a * x + b); // mod 2^32 implicitly
  std::memcpy(out, &y, 4);
}
