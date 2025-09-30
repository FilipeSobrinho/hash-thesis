// paralel_OPH_r1.cpp
// OPH accuracy on R1 (20B SHA-1), SAME dataset for all repetitions.
// Output CSV: function,rep,relerr   where relerr = (J_est - J_true) / J_true.
//
// Sketch API (yours):
//   - OPH::push(uint32_t hv)  // stores min hv per bin
//   - jaccard(OPH,OPH)        // compares two OPH sketches  :contentReference[oaicite:1]{index=1}
//
// Families: MSVec / TabOnMSVec / TornadoOnMSVecD4 / RapidHash32  (varlen -> 32-bit)

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
#include <unordered_set>
#include <vector>

#include "sketch/oph.hpp"
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

static double jaccard_true(const std::vector<Key20>& A, const std::vector<Key20>& B) {
    std::unordered_set<Key20, Key20Hash> As; As.reserve(A.size());
    std::unordered_set<Key20, Key20Hash> Bs; Bs.reserve(B.size());
    for (auto& k : A) As.insert(k);
    for (auto& k : B) Bs.insert(k);
    std::size_t inter = 0;
    if (As.size() < Bs.size()) { for (auto& k : As) inter += Bs.count(k); }
    else { for (auto& k : Bs) inter += As.count(k); }
    const std::size_t uni = As.size() + Bs.size() - inter;
    return uni ? double(inter) / double(uni) : 1.0;
}

int main(int argc, char** argv) {
    std::size_t K = 200;           // bins
    std::size_t R = 1'000;
    std::string outfile = "oph_r1_relerr.csv";
    unsigned threads = std::thread::hardware_concurrency(); if (!threads) threads = 4;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + a); };
        if (a == "--K") K = std::stoull(next());
        else if (a == "--R") R = std::stoull(next());
        else if (a == "--out") outfile = next();
        else if (a == "--threads") threads = (unsigned)std::stoul(next());
        else if (a == "--help" || a == "-h") {
            std::cout << "Usage: paralel_OPH_r1 --K 200 --R 50000 --out oph_r1.csv --threads N\n"; return 0;
        }
    }

    std::cout << "OPH on R1 (20B keys), same dataset for all reps\n"
        << "  K=" << K << "  R=" << R << "  threads=" << threads << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary); if (!out) { std::cerr << "Cannot open " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // Build the dataset once
    datasets::R1 base;
    const auto& raw = base.buffer();
    const std::size_t ITEMS = base.size();

    // Deterministic 50/50 position-based split (same for all reps)
    auto splitbit = [](std::uint64_t idx)->std::uint64_t {
        std::uint64_t x = idx + 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        return (x ^ (x >> 31)) & 1ull;
        };

    std::vector<Key20> A, B; A.reserve(ITEMS / 2 + 1024); B.reserve(ITEMS / 2 + 1024);
    for (std::size_t i = 0; i < ITEMS; ++i) {
        Key20 k; std::memcpy(k.b.data(), raw.data() + i * 20, 20);
        if (splitbit(i) == 0) A.push_back(k); else B.push_back(k);
    }

    const double J_true = jaccard_true(A, B);
    const double denom = (J_true > 0.0 ? J_true : 1.0);

    enum { IDX_MSVEC, IDX_TABMS, IDX_TOR4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MSVec", "TabOnMSVec", "TornadoOnMSVecD4", "RapidHash32" };

    using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;
    struct RepParams { Coeffs coeffs; std::uint64_t rapid_seed; };
    std::vector<RepParams> params(R);
    for (std::size_t r = 0; r < R; ++r) {
        for (std::size_t i = 0; i < MSVEC_NUM_COEFFS; ++i) params[r].coeffs[i] = rng::get_u64();
        params[r].rapid_seed = rng::get_u64();
    }

    std::mutex file_mtx, cout_mtx;
    std::atomic<std::size_t> done{ 0 };
    constexpr std::size_t PROG_STEP = 1000;

    auto worker = [&](unsigned tid) {
        std::ostringstream buf;
        for (std::size_t r = tid; r < R; r += threads) {
            // Per-rep families
            hashfn::MSVec msvec;             msvec.set_params(params[r].coeffs, true);
            hashfn::TabOnMSVec tabms;        tabms.set_params(params[r].coeffs, true);
            hashfn::TornadoOnMSVecD4 tor4;   tor4.set_params(params[r].coeffs, true);
            rapid::RapidHash32 rh32;         rh32.set_params(params[r].rapid_seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

            sketch::OPH SA[NUM_FUNCS] = { sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K),
                                          sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K) };
            sketch::OPH SB[NUM_FUNCS] = { sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K),
                                          sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K) };

            // Feed group A
            for (auto& k : A) {
                const void* p = k.b.data();
                SA[IDX_MSVEC].push(msvec.hash(p, 20));
                SA[IDX_TABMS].push(tabms.hash(p, 20));
                SA[IDX_TOR4].push(tor4.hash(p, 20));
                SA[IDX_RAPID32].push(rh32.hash(p, 20));
            }
            // Feed group B
            for (auto& k : B) {
                const void* p = k.b.data();
                SB[IDX_MSVEC].push(msvec.hash(p, 20));
                SB[IDX_TABMS].push(tabms.hash(p, 20));
                SB[IDX_TOR4].push(tor4.hash(p, 20));
                SB[IDX_RAPID32].push(rh32.hash(p, 20));
            }

            for (int f = 0; f < NUM_FUNCS; ++f) {
                const double J_est = sketch::jaccard(SA[f], SB[f]);
                const double rel = (J_est - J_true) / denom;
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
