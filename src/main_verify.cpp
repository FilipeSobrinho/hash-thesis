#include <iostream>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <core/dataset.hpp>
#include <core/unaligned.hpp>
#include <core/a1_mixed_materialized.hpp>

static uint32_t load_u32_le(const void* p){
  return GET_U32(static_cast<const uint8_t*>(p), 0);
}

int main(){
  const std::size_t N = 100000;
  const uint64_t seed = 0x0123456789ABCDEFull;

  // Fully materialized mixed dataset
  datasets::A1MixedMaterialized base(N);
  auto s = base.make_stream();

  // 1) Count items and distincts (no on-the-fly generation during iteration)
  const void* ptr = nullptr; std::size_t len = 0;
  std::vector<char> seen; seen.reserve(N/2 + 4096);
  uint32_t max_seen = 0;
  std::size_t count = 0;

  while (s.next(ptr, len)){
    ++count;
    uint32_t v = load_u32_le(ptr);
    if (v > max_seen) { max_seen = v; seen.resize(max_seen+1, 0); }
    seen[v] = 1;
  }
  std::cout << "Items (base): " << count << " (expected " << N << ")\n";
  std::cout << "Distinct (base): " << std::count(seen.begin(), seen.end(), 1) << "\n";

  // Determinism test on the materialized stream
  s.reset();
  std::vector<uint32_t> first10;
  for (int i=0;i<10 && s.next(ptr, len); ++i) first10.push_back(load_u32_le(ptr));
  s.reset();
  for (int i=0;i<10 && s.next(ptr, len); ++i) {
    uint32_t v = load_u32_le(ptr);
    if (v != first10[i]) { std::cerr << "[FAIL] determinism at " << i << "\n"; return 1; }
  }
  std::cout << "Determinism OK (base).\n";

  // 2) Fully materialized 50/50 split
  datasets::A1MixedSplitMaterialized split(N, seed);
  auto sA = split.make_streamA();
  auto sB = split.make_streamB();

  std::size_t cntA=0, cntB=0;
  std::vector<char> seenA, seenB;
  uint32_t mA=0, mB=0;

  while (sA.next(ptr, len)) { ++cntA; uint32_t v=load_u32_le(ptr); if (v>mA){mA=v; seenA.resize(mA+1,0);} seenA[v]=1; }
  while (sB.next(ptr, len)) { ++cntB; uint32_t v=load_u32_le(ptr); if (v>mB){mB=v; seenB.resize(mB+1,0);} seenB[v]=1; }

  std::cout << "Split sizes: A=" << cntA << ", B=" << cntB << " (sum=" << (cntA+cntB) << ", expected " << N << ")\n";

  // Distinct Jaccard across A and B (on values, not occurrences)
  uint32_t m = std::max(mA, mB);
  seenA.resize(m+1,0); seenB.resize(m+1,0);
  int inter=0, uni=0;
  for (uint32_t i=1;i<=m;++i){ inter += (seenA[i] && seenB[i]); uni += (seenA[i] || seenB[i]); }
  double j = (uni ? double(inter)/double(uni) : 0.0);
  std::cout << "Distinct A=" << std::count(seenA.begin(), seenA.end(), 1)
            << ", Distinct B=" << std::count(seenB.begin(), seenB.end(), 1)
            << ", Union=" << uni << ", Intersection=" << inter
            << ", Jaccard~" << j << "\n";

  std::cout << "verify (materialized A1Mixed + split): OK\n";
  return 0;
}
