#pragma once
#include <cstdint>
#include <cstring>

static inline uint16_t GET_U16(const uint8_t* b, uint32_t i) {
  uint16_t n;
  std::memcpy(&n, b + i, 2);
  return n;
}
static inline uint32_t GET_U32(const uint8_t* b, uint32_t i) {
  uint32_t n;
  std::memcpy(&n, b + i, 4);
  return n;
}
static inline uint64_t GET_U64(const uint8_t* b, uint32_t i) {
  uint64_t n;
  std::memcpy(&n, b + i, 8);
  return n;
}
