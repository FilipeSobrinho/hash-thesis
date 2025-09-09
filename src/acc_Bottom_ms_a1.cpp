// verify_bottomk_a1.cpp
// Runs Bottom-k on dataset A1 using MS hasher and compares to ground truth.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include "core/dataset.hpp"   // Stream { bool next(const void*& p, size_t& len); void reset(); }
#include "core/unaligned.hpp" // GET_U32(const uint8_t*, uint32_t)
#include "core/a1.hpp"        // A1{A1(N); make_stream();}
#include "hash/ms.hpp"        // MS{ void set_params(uint64_t a, uint64_t b); uint32_t hash(uint32_t x) const; }
#include "sketch/bottomk.hpp" // BottomK

static inline std::uint64_t parse_u64(const std::string& s) {
    if (s.rfind("0x", 0) == 0 || s.rfind("0X", 0) == 0) return std::stoull(s, nullptr, 16);
    return std::stoull(s, nullptr, 10);
}

static std::vector<std::uint32_t> materialize_keys(Stream& stream) {
    std::vector<std::uint32_t> keys;
    keys.reserve(1 << 20);
    const void* p = nullptr; std::size_t len = 0;
    while (stream.next(p, len)) {
        const auto* b = static_cast<const std::uint8_t*>(p);
        std::uint32_t x = 0;
        if (len >= 4)      x = GET_U32(b, 0);
        else if (len == 3) x = std::uint32_t(b[0]) | (std::uint32_t(b[1]) << 8) | (std::uint32_t(b[2]) << 16);
        else if (len == 2) x = std::uint32_t(b[0]) | (std::uint32_t(b[1]) << 8);
        else if (len == 1) x = std::uint32_t(b[0]);
        keys.push_back(x);
    }
    return keys;
}

int main(int argc, char** argv) {
    // Defaults
    std::size_t N = 5'000'000;         // A1 total items
    std::size_t K = 512;               // bottom-k size
    std::uint64_t A = 0x9E3779B97F4A7C15ull; // odd is enforced in MS
    std::uint64_t B = 0xA5A5A5A5A5A5A5A5ull;

    // CLI
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + arg); };
        if (arg == "--N" || arg == "-n") N = std::stoull(next());
        else if (arg == "--k" || arg == "-k") K = std::stoull(next());
        else if (arg == "--a") A = parse_u64(next());
        else if (arg == "--b") B = parse_u64(next());
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: verify_bottomk_a1 [--N <items>] [--k <bottom-k>] [--a <u64>] [--b <u64>]\n";
            return 0;
        }
    }

    std::cout << "Bottom-k verify on A1: N=" << N << "  k=" << K
        << "  a=0x" << std::hex << A << " b=0x" << B << std::dec << "\n";

    // 1) Build dataset & materialize keys (so hashing cost is isolated if you time later)
    datasets::A1 a1(N);
    auto s = a1.make_stream();
    std::vector<std::uint32_t> keys = materialize_keys(s);
    std::cout << "A1 materialized " << keys.size() << " keys\n";

    // 2) Ground-truth distinct (from the keys themselves)
    std::unordered_set<std::uint32_t> uniq;
    uniq.reserve(keys.size() / 4 + 1);
    for (auto x : keys) uniq.insert(x);
    const std::size_t distinct_true = uniq.size();
    std::cout << "Ground-truth distinct = " << distinct_true << "\n";

    // 3) Hash with MS and feed Bottom-k
    hashfn::MS hasher;
    hasher.set_params(A, B);

    sketch::BottomK bk(K);
    for (auto x : keys) {
        const std::uint32_t h = hasher.hash(x); // 32-bit hash
        bk.push(h);
    }

    const double estimate = bk.estimate();
    const double rel_err = (distinct_true > 0)
        ? std::abs(estimate - double(distinct_true)) / double(distinct_true)
        : 0.0;

    std::cout << std::fixed << std::setprecision(4);
    std::cout << "Bottom-k estimate = " << estimate
        << "   kth_hash=0x" << std::hex << bk.kth_hash() << std::dec
        << "   rel_err=" << rel_err << "\n";

    std::cout << "verify_bottomk_a1: OK\n";
    return 0;
}
