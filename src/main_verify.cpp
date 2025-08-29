#include <iostream>
#include <vector>
#include <algorithm>
#include <span>
#include <cstdint>
#include <core/dataset.hpp>
#include <core/a1.hpp>

static uint32_t load_u32(std::span<const std::byte> s){
  uint32_t v = 0;
  v |= uint32_t(std::to_integer<unsigned char>(s[0]));
  v |= uint32_t(std::to_integer<unsigned char>(s[1])) << 8;
  v |= uint32_t(std::to_integer<unsigned char>(s[2])) << 16;
  v |= uint32_t(std::to_integer<unsigned char>(s[3])) << 24;
  return v;
}

int main(){
  const std::size_t N = 100000; // total items desired
  A1TotalStream a1(N);
  std::span<const std::byte> key;
  std::size_t count = 0;
  uint64_t sum = 0;

  while (a1.next(key)){
    ++count;
    sum += load_u32(key);
  }
  std::cout << "A1 (no-max) total items: " << count << " (expected " << N << ")\n";
  if (count != N) { std::cerr << "[FAIL] count mismatch\n"; return 1; }

  // Determinism check
  a1.reset();
  std::vector<uint32_t> first10;
  for (int i=0;i<10 && a1.next(key);++i) first10.push_back(load_u32(key));
  a1.reset();
  for (int i=0;i<10 && a1.next(key);++i){
    if (first10[i] != load_u32(key)) { std::cerr << "[FAIL] determinism\n"; return 1; }
  }
  std::cout << "Determinism OK. First 10 stable.\n";

  // 50/50 split
  const uint64_t seed = 123456789ull;
  A1TotalSplitStream sA(N, seed, 0);
  A1TotalSplitStream sB(N, seed, 1);
  // Track distinct keys in each split (keys are in [1, ..] without fixed max)
  // We'll approximate by recording up to a high-water mark seen.
  uint32_t max_seen = 0;
  std::vector<char> seenA, seenB;

  auto ensure = [&](uint32_t v){
    if (v > max_seen){
      max_seen = v;
      seenA.resize(max_seen+1, 0);
      seenB.resize(max_seen+1, 0);
    }
  };

  while (sA.next(key)) { uint32_t v = load_u32(key); ensure(v); seenA[v]=1; }
  while (sB.next(key)) { uint32_t v = load_u32(key); ensure(v); seenB[v]=1; }

  int inter=0, uni=0;
  for (uint32_t i=1;i<=max_seen;++i){
    const bool a = i < seenA.size() ? seenA[i] : 0;
    const bool b = i < seenB.size() ? seenB[i] : 0;
    inter += (a && b);
    uni   += (a || b);
  }
  std::cout << "Distinct A: " << std::count(seenA.begin(), seenA.end(), 1)
            << ", Distinct B: " << std::count(seenB.begin(), seenB.end(), 1)
            << ", Union: " << uni << ", Intersection: " << inter
            << ", Jaccard ~ " << (uni ? (double)inter/uni : 0.0) << "\n";

  std::cout << "verify: A1 (no-max) OK\n";
  return 0;
}
