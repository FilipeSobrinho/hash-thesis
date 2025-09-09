// src/accuracy_bottomk_ms_a1.cpp
// 5e4 repetitions, k=24500, true D=5e5; using MS hasher on distinct keys 1..D.
// Writes one relative error per line to a txt file for plotting.

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "hash/ms.hpp"          // MS: set_params(uint64_t a, uint64_t b); uint32_t hash(uint32_t x) const;
#include "sketch/bottomk.hpp"   // sketch::BottomK
#include "core/randomgen.hpp"   // rng::get_u64() for deterministic reseeding

static inline std::uint64_t parse_u64(const std::string& s) {
    if (s.rfind("0x", 0) == 0 || s.rfind("0X", 0) == 0) return std::stoull(s, nullptr, 16);
    return std::stoull(s, nullptr, 10);
}

int main(int argc, char** argv) {
    // Defaults per your spec
    const std::size_t D_default = 500000;    // cardinality
    const std::size_t K_default = 24500;     // bottom-k
    const std::size_t R_default = 50000;     // repetitions
    std::size_t D = D_default, K = K_default, R = R_default;
    std::string outfile = "bottomk_ms_a1_relerr.txt";
    std::uint64_t a_seed0 = 0, b_seed0 = 0; // optional fixed seeds (0 -> use rng)

    // CLI
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + arg); };
        if (arg == "--D") D = std::stoull(next());
        else if (arg == "--k") K = std::stoull(next());
        else if (arg == "--R") R = std::stoull(next());
        else if (arg == "--out") outfile = next();
        else if (arg == "--a0") a_seed0 = parse_u64(next());
        else if (arg == "--b0") b_seed0 = parse_u64(next());
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: accuracy_bottomk_ms_a1 [--D 500000] [--k 24500] [--R 50000] [--out file.txt] [--a0 <u64>] [--b0 <u64>]\n";
            return 0;
        }
    }

    std::cout << "Bottom-k accuracy (MS on A1 distinct keys): D=" << D
        << "  k=" << K << "  R=" << R << "\n"
        << "Writing relative errors to: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) {
        std::cerr << "Cannot open output file: " << outfile << "\n";
        return 1;
    }
    out.setf(std::ios::fixed); out << std::setprecision(8);

    // Precompute distinct key universe (1..D), as A1's duplicates don't affect distinct counting accuracy
    std::vector<std::uint32_t> keys;
    keys.reserve(D);
    for (std::uint32_t i = 1; i <= D; ++i) keys.push_back(i);

    // Repetitions
    for (std::size_t r = 0; r < R; ++r) {
        // New hash parameters each rep (deterministic across clones via rng loader)
        std::uint64_t a = (a_seed0 ? a_seed0 + r : rng::get_u64());
        std::uint64_t b = (b_seed0 ? b_seed0 + r : rng::get_u64());

        hashfn::MS h; h.set_params(a, b);
        sketch::BottomK bk(K);

        // Feed distinct keys once each
        for (auto x : keys) {
            std::uint32_t hv = h.hash(x);
            bk.push(hv);
        }

        const double est = bk.estimate();
        const double relerr = (est - double(D)) / double(D);
        out << relerr << "\n";

        if ((r + 1) % 1000 == 0) {
            std::cout << "  rep " << (r + 1) << "/" << R << " done\r" << std::flush;
        }
    }

    std::cout << "\nDone. Wrote " << R << " relative errors to " << outfile << "\n";
    return 0;
}
