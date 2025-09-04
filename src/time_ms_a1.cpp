// bench_timing_ms_a1.cpp
// Pure hashing benchmark for MS (multiply-shift) on A1 and A1Split.
// - Pre-materializes keys outside the timing loop.
// - Measures steady_clock elapsed time for hashing only.
//
// Expected headers in your repo:
//   include/core/dataset.hpp   -> struct Stream { bool next(const void*& p, size_t& len); void reset(); };
//   include/core/unaligned.hpp -> GET_U32(const uint8_t*, uint32_t)
//   include/core/a1.hpp        -> struct A1{ explicit A1(size_t N); Stream make_stream() const; }
//                                 struct A1Split{ A1Split(size_t N, uint64_t seed);
//                                                   Stream make_streamA() const; Stream make_streamB() const; }
//   include/hash/ms.hpp        -> struct MS{ void set_params(uint64_t a, uint64_t b); uint32_t hash(uint32_t x) const; }

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include "core/dataset.hpp"
#include "core/unaligned.hpp"
#include "core/a1.hpp"
#include "hash/ms.hpp"

using Steady = std::chrono::steady_clock;

struct RunStats {
    double ns_per_key{};
    double mkeys_per_s{};
    double seconds{};
    uint32_t sink{}; // keep compiler from eliding the loop
};

static inline uint64_t parse_u64(const std::string& s) {
    if (s.rfind("0x", 0) == 0 || s.rfind("0X", 0) == 0) return std::stoull(s, nullptr, 16);
    return std::stoull(s, nullptr, 10);
}

static std::vector<uint32_t> materialize_keys(Stream& stream) {
    std::vector<uint32_t> keys;
    keys.reserve(1 << 20);
    const void* p = nullptr;
    std::size_t len = 0;
    while (stream.next(p, len)) {
        const uint8_t* b = static_cast<const uint8_t*>(p);
        uint32_t x = 0;
        if (len >= 4) {
            x = GET_U32(b, 0);  // alignment-safe
        }
        else if (len == 3) {
            x = uint32_t(b[0]) | (uint32_t(b[1]) << 8) | (uint32_t(b[2]) << 16);
        }
        else if (len == 2) {
            x = uint32_t(b[0]) | (uint32_t(b[1]) << 8);
        }
        else if (len == 1) {
            x = uint32_t(b[0]);
        } // len==0 -> x stays 0
        keys.push_back(x);
    }
    return keys;
}

static RunStats bench_ms(const std::vector<uint32_t>& keys, const hashfn::MS& h) {
    // quick warmup
    volatile uint32_t sink = 0;
    for (size_t i = 0; i < keys.size() && i < 100000; i++) sink ^= h.hash(keys[i]);

    auto t0 = Steady::now();
    uint32_t acc = 0;
    for (uint32_t x : keys) acc ^= h.hash(x);
    auto t1 = Steady::now();

    const double secs = std::chrono::duration<double>(t1 - t0).count();
    const double ns_per_key = (secs * 1e9) / (keys.empty() ? 1.0 : double(keys.size()));
    const double mkeys_per_s = (secs > 0) ? (keys.size() / 1e6) / secs : 0.0;
    return { ns_per_key, mkeys_per_s, secs, uint32_t(acc) ^ uint32_t(sink) };
}

static void summarize(const char* label, const std::vector<RunStats>& rs, size_t nkeys) {
    auto median = [](std::vector<double> v) {
        if (v.empty()) return 0.0;
        std::sort(v.begin(), v.end());
        const size_t m = v.size() / 2;
        return (v.size() % 2) ? v[m] : 0.5 * (v[m - 1] + v[m]);
        };
    std::vector<double> nsks, mks;
    for (auto& r : rs) { nsks.push_back(r.ns_per_key); mks.push_back(r.mkeys_per_s); }
    const double med_ns = median(nsks);
    const double med_mk = median(mks);
    const double mean_ns = std::accumulate(nsks.begin(), nsks.end(), 0.0) / (nsks.empty() ? 1 : nsks.size());
    const double mean_mk = std::accumulate(mks.begin(), mks.end(), 0.0) / (mks.empty() ? 1 : mks.size());

    std::cout << "\n[" << label << "] keys=" << nkeys
        << "  median: " << std::fixed << std::setprecision(2) << med_ns << " ns/key, "
        << std::setprecision(2) << med_mk << " Mkeys/s"
        << "  |  mean: " << std::setprecision(2) << mean_ns << " ns/key, "
        << std::setprecision(2) << mean_mk << " Mkeys/s"
        << std::defaultfloat << "\n";
    for (size_t i = 0; i < rs.size(); ++i) {
        std::cout << "  run " << (i + 1) << ": "
            << std::fixed << std::setprecision(2) << rs[i].ns_per_key << " ns/key, "
            << std::setprecision(2) << rs[i].mkeys_per_s << " Mkeys/s, "
            << std::defaultfloat << rs[i].seconds << " s (sink=" << rs[i].sink << ")\n";
    }
}

int main(int argc, char** argv) {
    // Defaults
    size_t N = 10'000'000;       // total items for A1
    int reps = 5;
    bool do_split = true;
    uint64_t A = 0x9E3779B97F4A7C15ull; // odd
    uint64_t B = 0xA5A5A5A5A5A5A5A5ull;
    uint64_t split_seed = 0x0123456789ABCDEFull;

    // Parse args
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + arg); };
        if (arg == "--N" || arg == "-n") N = std::stoull(next());
        else if (arg == "--reps") reps = std::stoi(next());
        else if (arg == "--nosplit") do_split = false;
        else if (arg == "--a") A = parse_u64(next());
        else if (arg == "--b") B = parse_u64(next());
        else if (arg == "--split-seed") split_seed = parse_u64(next());
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: bench_timing_ms_a1 [--N <items>] [--reps <R>] [--nosplit] [--a <u64>] [--b <u64>] [--split-seed <u64>]\n";
            return 0;
        }
    }

    std::cout << "MS timing on A1/A1Split  N=" << N << "  reps=" << reps
        << "  a=0x" << std::hex << A << " b=0x" << B << std::dec << "\n";

    // Build datasets and materialize keys (outside timing)
    datasets::A1 a1(N);
    auto s_base = a1.make_stream();
    auto keys_base = materialize_keys(s_base);
    std::cout << "A1 materialized: " << keys_base.size() << " keys\n";

    std::vector<uint32_t> keys_A, keys_B;
    if (do_split) {
        datasets::A1Split a1s(N, split_seed);
        auto sA = a1s.make_streamA();
        auto sB = a1s.make_streamB();
        keys_A = materialize_keys(sA);
        keys_B = materialize_keys(sB);
        std::cout << "A1Split materialized: A=" << keys_A.size() << "  B=" << keys_B.size()
            << " (sum=" << (keys_A.size() + keys_B.size()) << ")\n";
    }

    // Prepare hasher
    hashfn::MS h;
    h.set_params(A, B);  // your MS enforces odd 'a' internally

    auto do_runs = [&](const char* label, const std::vector<uint32_t>& keys) {
        std::vector<RunStats> rs;
        rs.reserve(reps);
        for (int r = 0; r < reps; ++r) rs.push_back(bench_ms(keys, h));
        summarize(label, rs, keys.size());
        };

    do_runs("A1 (base)", keys_base);
    if (do_split) {
        do_runs("A1Split (A)", keys_A);
        do_runs("A1Split (B)", keys_B);
    }

    std::cout << "\nbench_timing_ms_a1: DONE\n";
    return 0;
}
