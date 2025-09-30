// paralel_CM_r1.cpp
// Count–Min accuracy on R1 (20-byte SHA-1 items) with MSVec-based families (RapidHash as var-len exception).
// Output CSV: function,rep,relerr   where relerr = mean over DISTINCT keys of ( (est - true) / true ).
//
// Sketch API (yours):
//   - CountMin::add(uint32_t key, uint32_t count=1)
//   - CountMin::estimate(uint32_t key) const
//   - set_row(row, hasher*) where hasher has uint32_t hash(uint32_t) const
//
// Hash pipeline here:  (20B key) --family--> 32-bit hv  --CMS row hash--> column index.
//
// Families:
//   - MSVec                 : varlen -> 32-bit
//   - TabOnMSVec            : varlen -> SimpleTab32(32-bit)
//   - TornadoOnMSVecD4      : varlen -> Tornado(32-bit, D=4)
//   - RapidHash32           : varlen -> top-32 of 64-bit RapidHash
//
// Per repetition:
//   - New MSVec coefficients (shared by MSVec/Tab/Tornado prehash) and RapidHash seed.
//   - New per-row row-hashers for CMS (simple 32-bit affine mixer with different seeds).

#include <algorithm>
#include <atomic>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "sketch/countmin.hpp"
#include "core/randomgen.hpp"
#include "hash/msvec.hpp"           // hashfn::MSVec (varlen -> 32-bit) [MSVEC_NUM_COEFFS]
#include "hash/simpletab32.hpp"     // hashfn::TabOnMSVec
#include "hash/tornado32.hpp"       // hashfn::TornadoOnMSVecD4
#include "hash/rapidhash.h"         // rapid::RapidHash32 (varlen)
#include "core/r1.hpp"                // datasets::R1 (20B items)

struct Key20 {
    std::array<std::uint8_t, 20> b{};
    bool operator==(const Key20& o) const noexcept { return std::memcmp(b.data(), o.b.data(), 20) == 0; }
};
struct Key20Hash {
    std::size_t operator()(const Key20& k) const noexcept {
        std::uint32_t h = 2166136261u; for (auto x : k.b) { h ^= x; h *= 16777619u; } return (std::size_t)h;
    }
};

// Simple per-row 32-bit mixer for CMS: hv' = (a*hv + b), with a odd.
struct RowHash32 {
    std::uint32_t a = 0, b = 0;
    uint32_t hash(uint32_t x) const { return a * x + b; }
    static RowHash32 random() {
        RowHash32 r;
        r.a = (uint32_t)rng::get_u64() | 1u; // odd
        r.b = (uint32_t)rng::get_u64();
        return r;
    }
};

int main(int argc, char** argv) {
    std::size_t WIDTH = 32'768;
    std::size_t DEPTH = 3;
    std::size_t R = 1'000;
    std::string outfile = "cms_r1_relerr.csv";
    unsigned threads = std::thread::hardware_concurrency();
    if (!threads) threads = 4;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + a); };
        if (a == "--width") WIDTH = std::stoull(next());
        else if (a == "--depth") DEPTH = std::stoull(next());
        else if (a == "--R") R = std::stoull(next());
        else if (a == "--out") outfile = next();
        else if (a == "--threads") threads = (unsigned)std::stoul(next());
        else if (a == "--help" || a == "-h") {
            std::cout <<
                "Usage: paralel_CM_r1 --width 32768 --depth 3 --R 50000 --out cms_r1.csv --threads N\n"; return 0;
        }
    }

    std::cout << "CMS on R1 (20B SHA-1)\n"
        << "  width=" << WIDTH << " depth=" << DEPTH
        << "  R=" << R << "  threads=" << threads << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary); if (!out) { std::cerr << "Cannot open " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // Build dataset once
    datasets::R1 base;
    const auto& raw = base.buffer();
    const std::size_t ITEMS = base.size();

    // True frequencies over DISTINCT 20B keys
    std::unordered_map<Key20, std::uint32_t, Key20Hash> freq;
    freq.reserve(ITEMS);
    for (std::size_t i = 0; i < ITEMS; ++i) { Key20 k; std::memcpy(k.b.data(), raw.data() + i * 20, 20); ++freq[k]; }
    std::vector<Key20> distinct; distinct.reserve(freq.size());
    for (auto& kv : freq) distinct.push_back(kv.first);

    enum { IDX_MSVEC, IDX_TABMS, IDX_TOR4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MSVec", "TabOnMSVec", "TornadoOnMSVecD4", "RapidHash32" };

    using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;
    struct RepParams { Coeffs coeffs; std::uint64_t rapid_seed; std::vector<RowHash32> rows; };

    // Pre-generate repetition params
    std::vector<RepParams> params(R);
    for (std::size_t r = 0; r < R; ++r) {
        for (std::size_t i = 0; i < MSVEC_NUM_COEFFS; ++i) params[r].coeffs[i] = rng::get_u64();
        params[r].rapid_seed = rng::get_u64();
        params[r].rows.resize(DEPTH);
        for (std::size_t d = 0; d < DEPTH; ++d) params[r].rows[d] = RowHash32::random();
    }

    auto mean_additive_relerr = [&](auto&& cms, auto&& family_hash32)->double {
        long double sum = 0.0L;
        for (const auto& k : distinct) {
            const auto it = freq.find(k);
            const std::uint32_t truef = it->second;
            const std::uint32_t hv = family_hash32(k.b.data(), 20);
            const std::uint32_t est = cms.estimate(hv);
            sum += ((long double)est - (long double)truef) / (long double)truef;
        }
        return (distinct.empty() ? 0.0 : (double)(sum / (long double)distinct.size()));
        };

    std::mutex file_mtx, cout_mtx;
    std::atomic<std::size_t> done{ 0 };
    constexpr std::size_t PROG_STEP = 1000;

    auto worker = [&](unsigned tid) {
        std::ostringstream buf;
        for (std::size_t r = tid; r < R; r += threads) {
            // Build per-rep prehash families
            hashfn::MSVec msvec;             msvec.set_params(params[r].coeffs, true);
            hashfn::TabOnMSVec tabms;        tabms.set_params(params[r].coeffs, true);
            hashfn::TornadoOnMSVecD4 tor4;   tor4.set_params(params[r].coeffs, true);
            rapid::RapidHash32 rh32;         rh32.set_params(params[r].rapid_seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

            // For each family, build a CMS with DEPTH rows and set per-row hashers
            for (int f = 0; f < NUM_FUNCS; ++f) {
                sketch::CountMin cms(WIDTH, DEPTH);
                // Register row hashers
                for (std::size_t d = 0; d < DEPTH; ++d) cms.set_row(d, &params[r].rows[d]);

                // Ingest stream once for this family
                auto family_hash32 = [&](const void* p, std::size_t len)->std::uint32_t {
                    switch (f) {
                    case IDX_MSVEC:   return msvec.hash(p, len);
                    case IDX_TABMS:   return tabms.hash(p, len);
                    case IDX_TOR4:    return tor4.hash(p, len);
                    case IDX_RAPID32: return rh32.hash(p, len);
                    default: return 0;
                    }
                    };
                for (std::size_t i = 0; i < ITEMS; ++i) {
                    const void* p = raw.data() + i * 20;
                    const std::uint32_t hv = family_hash32(p, 20);
                    cms.add(hv, 1);
                }

                const double rel = mean_additive_relerr(cms, family_hash32);
                buf << NAMES[f] << "," << (r + 1) << "," << rel << "\n";
            }

            std::size_t n = done.fetch_add(1, std::memory_order_relaxed) + 1;
            if ((n % PROG_STEP) == 0 || n == R) {
                std::lock_guard<std::mutex> io(cout_mtx);
                std::cout << "  rep " << n << " / " << R
                    << " (" << (100.0 * double(n) / double(R)) << "%)\r" << std::flush;
            }
        }
        std::lock_guard<std::mutex> g(file_mtx);
        out << buf.str();
        };

    std::vector<std::thread> pool; pool.reserve(threads);
    for (unsigned t = 0; t < threads; ++t) pool.emplace_back(worker, t);
    for (auto& th : pool) th.join();

    { std::lock_guard<std::mutex> io(cout_mtx); std::cout << "\n"; }
    std::cout << "Done.\n";
    return 0;
}
