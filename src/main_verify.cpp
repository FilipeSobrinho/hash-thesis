#include <iostream>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <core/dataset.hpp>
#include <core/unaligned.hpp>
#include <core/a1_mixed_stream.hpp>
#include <hash/ms32_demo.hpp>

static uint32_t load_u32_le(const void* p){
  return GET_U32(static_cast<const uint8_t*>(p), 0);
}

int main(){
  const std::size_t N = 100000;
  datasets::A1MixedStream s(N);

  // 1) Count items and distincts using ptr/len
  const void* ptr = nullptr; std::size_t len = 0;
  std::vector<char> seen; seen.reserve(N/2 + 1024);
  uint32_t max_seen = 0;
  std::size_t count = 0;

  while (s.next(ptr, len)){
    ++count;
    // All keys are 4 bytes LE, but we consume via (ptr,len)
    uint32_t v = load_u32_le(ptr);
    if (v > max_seen) { max_seen = v; seen.resize(max_seen+1, 0); }
    seen[v] = 1;
  }
  std::cout << "Items: " << count << " (expected " << N << ")\n";
  std::cout << "Distinct: " << std::count(seen.begin(), seen.end(), 1) << "\n";

  // 2) Determinism: first 10 items must match after reset
  s.reset();
  std::vector<uint32_t> first10;
  for (int i=0;i<10 && s.next(ptr, len); ++i) first10.push_back(load_u32_le(ptr));
  s.reset();
  for (int i=0;i<10 && s.next(ptr, len); ++i) {
    uint32_t v = load_u32_le(ptr);
    if (v != first10[i]) { std::cerr << "[FAIL] determinism at " << i << "\n"; return 1; }
  }
  std::cout << "Determinism OK.\n";

  // 3) Demonstrate using a C-style hash: ms32_demo(in,len,seed,out)
  s.reset();
  uint64_t seed = 0x0123456789ABCDEFull;
  uint32_t out32 = 0;
  for (int i=0;i<5 && s.next(ptr, len); ++i) {
    ms32_demo(ptr, len, seed, &out32);
    std::cout << "key" << (i+1) << " -> hash32 = 0x" << std::hex << out32 << std::dec << "\n";
  }

  std::cout << "verify (ptr,len + A1Mixed): OK\n";
  return 0;
}
