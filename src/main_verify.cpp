// src/accuracy_bottomk_all_a1.cpp
// Compare multiple hash functions on Bottom-k for distinct counting.
// Produces a single CSV with columns: function,rep,relerr
//
// Default: D=5e5, k=24500, R=5e4 (be mindful: this is heavy).
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "sketch/bottomk.hpp"
#include "core/randomgen.hpp"
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"

static inline std::uint64_t parse_u64(const std::string& s) {
    if (s.rfind("0x", 0) == 0 || s.rfind("0X", 0) == 0) return std::stoull(s, nullptr, 16);
    return std::stoull(s, nullptr, 10);
}

int main(int argc, char** argv) {
    const std::size_t D_default = 500000;   // cardinality
    const std::size_t K_default = 24500;    // bottom-k size
    const std::size_t R_default = 2000;    // repetitions
    std::size_t D = D_default, K = K_default, R = R_default;
    std::string outfile = "bottomk_all_relerr.csv";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + arg); };
        if (arg == "--D") D = std::stoull(next());
        else if (arg == "--k") K = std::stoull(next());
        else if (arg == "--R") R = std::stoull(next());
        else if (arg == "--out") outfile = next();
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: accuracy_bottomk_all_a1 [--D 500000] [--k 24500] [--R 50000] [--out file.csv]\n";
            return 0;
        }
    }

    std::cout << "Bottom-k accuracy (ALL) on distinct 1..D\n"
        << "  D=" << D << "  k=" << K << "  R=" << R << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out << "function,rep,relerr\n";
    out.setf(std::ios::fixed); out << std::setprecision(8);

    // Precompute distinct keys 1..D
    std::vector<std::uint32_t> keys; keys.reserve(D);
    for (std::uint32_t i = 1; i <= D; ++i) keys.push_back(i);

    // Hash families we test
    enum { IDX_MS, IDX_STAB, IDX_TORNADO_D4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MultShift", "SimpleTab", "TornadoD4", "RapidHash32" };

    for (std::size_t r = 0; r < R; ++r) {
        // --- instantiate each hasher with fresh parameters ---
        // 1) Multiply Shift
        hashfn::MS h_ms; {
            std::uint64_t a = rng::get_u64();
            std::uint64_t b = rng::get_u64();
            h_ms.set_params(a, b);
        }
        // 2) Simple Tabulation (seed -> table via Poly32 degree=100)
        hashfn::SimpleTab32 h_stab; {
            std::uint64_t seed = rng::get_u64();
            h_stab.set_params(seed);
        }
        // 3) Tornado D4
        hashfn::TornadoTab32D4 h_tor4; {
            std::uint64_t seed = rng::get_u64();
            h_tor4.set_params(seed);
        }
        // 4) RapidHash top-32
        rapid::RapidHash32 h_rapid; {
            std::uint64_t seed = rng::get_u64();
            h_rapid.set_params(seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);
        }

        // --- run Bottom-k per hasher ---
        auto run_one = [&](int idx) {
            sketch::BottomK bk(K);
            switch (idx) {
            case IDX_MS: {
                for (auto x : keys) { std::uint32_t hv = h_ms.hash(x); bk.push(hv); }
            } break;
            case IDX_STAB: {
                for (auto x : keys) { std::uint32_t hv = h_stab.hash(x); bk.push(hv); }
            } break;
            case IDX_TORNADO_D4: {
                for (auto x : keys) { std::uint32_t hv = h_tor4.hash(x); bk.push(hv); }
            } break;
            case IDX_RAPID32: {
                for (auto x : keys) { std::uint32_t hv = h_rapid.hash(&x, sizeof(x)); bk.push(hv); }
            } break;
            }
            const double est = bk.estimate();
            return (est - double(D)) / double(D);
            };

        for (int f = 0; f < NUM_FUNCS; ++f) {
            const double relerr = run_one(f);
            out << NAMES[f] << "," << (r + 1) << "," << relerr << "\n";
        }

        if ((r + 1) % 1000 == 0) {
            std::cout << "  rep " << (r + 1) << "/" << R << " done\r" << std::flush;
        }
    }

    std::cout << "\nDone.\n";
    return 0;
}
