#include <iostream>
#include <vector>
#include <algorithm>
#include <span>
#include <cstdint>
#include <core/dataset.hpp>
#include <core/a1_mixed_stream.hpp>

static uint32_t load_u32(std::span<const std::byte> s){
  uint32_t v = 0;
  v |= uint32_t(std::to_integer<unsigned char>(s[0]));
  v |= uint32_t(std::to_integer<unsigned char>(s[1])) << 8;
  v |= uint32_t(std::to_integer<unsigned char>(s[2])) << 16;
  v |= uint32_t(std::to_integer<unsigned char>(s[3])) << 24;
  return v;
}

int main(){
  const std::size_t N = 100000;
  datasets::A1MixedStream mix(N);
  std::span<const std::byte> key;
  std::size_t count = 0;
  uint32_t max_seen = 0;

  std::vector<char> seen; seen.reserve(N/2 + 1024);

  while (mix.next(key)){
    ++count;
    uint32_t v = load_u32(key);
    if (v > max_seen){ max_seen = v; seen.resize(max_seen+1, 0); }
    seen[v] = 1;
  }
  std::cout << "Mixed total items: " << count << " (expected " << N << ")\n";
  std::cout << "Distinct overall: " << std::count(seen.begin(), seen.end(), 1) << "\n";

  // Determinism check
  mix.reset();
  std::vector<uint32_t> first10;
  for (int i=0;i<10 && mix.next(key);++i) first10.push_back(load_u32(key));
  mix.reset();
  for (int i=0;i<10 && mix.next(key);++i){
    if (first10[i] != load_u32(key)) {
      std::cerr << "[FAIL] determinism mismatch at " << i << "\n";
      return 1;
    }
  }
  std::cout << "Determinism OK.\n";

  // 50/50 split test
  const uint64_t seed = 123456789ull;
  datasets::A1MixedSplitStream A(N, seed, 0);
  datasets::A1MixedSplitStream B(N, seed, 1);

  uint32_t mA=0, mB=0;
  std::vector<char> seenA, seenB;
  while (A.next(key)){ uint32_t v = load_u32(key); if (v>mA){mA=v; seenA.resize(mA+1,0);} seenA[v]=1; }
  while (B.next(key)){ uint32_t v = load_u32(key); if (v>mB){mB=v; seenB.resize(mB+1,0);} seenB[v]=1; }

  int inter=0, uni=0;
  uint32_t m = std::max(mA, mB);
  seenA.resize(m+1,0); seenB.resize(m+1,0);
  for (uint32_t i=1;i<=m;++i){ inter += (seenA[i] && seenB[i]); uni += (seenA[i] || seenB[i]); }
  std::cout << "Distinct A: " << std::count(seenA.begin(), seenA.end(), 1)
            << ", Distinct B: " << std::count(seenB.begin(), seenB.end(), 1)
            << ", Union: " << uni << ", Intersection: " << inter
            << ", Jaccard ~ " << (uni ? (double)inter/uni : 0.0) << "\n";

  std::cout << "verify: A1Mixed OK\n";
  return 0;
}
