// serial_BK_a1.cpp
// Bottom-k accuracy, single-threaded, A1 dataset.
// CSV: function,rep,relerr
// Includes Tornado D1..D4 variants.
//
// Hashers (32-bit):
//   - MultShift
//   - SimpleTab32
//   - TornadoTab32D1/D2/D3/D4
//   - RapidHash32
//
// CLI:
//   --items N   (alias --D) default 500000
//   --k K       default 24500
//   --R R       repetitions (default 1000)
//   --out FILE  output CSV
//   --help

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "sketch/bottomk.hpp"
#include "core/randomgen.hpp"       // rng::get_u64()
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"       // expects D1..D4 variants to be available
#include "hash/rapidhash.h"
#include "core/a1.hpp"

static inline std::uint32_t load_le_u32(const void* p) {
    const std::uint8_t* b = static_cast<const std::uint8_t*>(p);
    return (std::uint32_t)b[0] | (std::uint32_t(b[1]) << 8)
        | (std::uint32_t(b[2]) << 16) | (std::uint32_t(b[3]) << 24);
}

int main(int argc, char** argv) {
    // Defaults
    std::size_t ITEMS = 500'000;
    std::size_t K = 24'500;
    std::size_t R = 50'000;
    std::string outfile = "bottomk_a1_relerr.csv";

    // Parse CLI
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 < argc) return std::string(argv[++i]);
            throw std::runtime_error("Missing value for " + arg);
        };
        if (arg == "--items" || arg == "--D") ITEMS = std::stoull(next());
        else if (arg == "--k") K = std::stoull(next());
        else if (arg == "--R") R = std::stoull(next());
        else if (arg == "--out") outfile = next();
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: serial_BK_a1 [--items 500000] [--k 24500] [--R 1000] [--out file.csv]\n";
            return 0;
        }
    }

    std::cout << "Bottom-k accuracy (A1) single-threaded\n"
              << "  items=" << ITEMS << "  k=" << K << "  R=" << R << "\n"
              << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // Build dataset once
    datasets::A1 base(ITEMS);
    std::vector<std::uint32_t> keys; keys.reserve(ITEMS);
    {
        auto st = base.make_stream();
        const void* p; std::size_t len;
        while (st.next(p, len)) keys.push_back(load_le_u32(p));
    }

    // Dtrue
    std::unordered_set<std::uint32_t> uniq; uniq.reserve(keys.size());
    for (auto v : keys) uniq.insert(v);
    const double Dtrue = static_cast<double>(uniq.size());

    enum {
        IDX_MS,
        IDX_STAB,
        IDX_TOR_D1,
        IDX_TOR_D2,
        IDX_TOR_D3,
        IDX_TOR_D4,
        IDX_RAPID32,
        NUM_FUNCS
    };
    const char* NAMES[NUM_FUNCS] = {
        "MultShift","SimpleTab","TornadoD1","TornadoD2","TornadoD3","TornadoD4","RapidHash32"
    };

    struct RepParams { std::uint64_t ms_a, ms_b, rapid_seed; };
    std::vector<RepParams> params(R);
    for (std::size_t r = 0; r < R; ++r) {
        params[r].ms_a = rng::get_u64();
        params[r].ms_b = rng::get_u64();
        params[r].rapid_seed = rng::get_u64();
    }

    for (std::size_t r = 0; r < R; ++r) {
        hashfn::MS h_ms; h_ms.set_params(params[r].ms_a, params[r].ms_b);

        rapid::RapidHash32 h_rapid;
        h_rapid.set_params(params[r].rapid_seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

        // Seed fresh tables per repetition
        hashfn::SimpleTab32    h_stab;     h_stab.set_params();
        hashfn::TornadoTab32D1 h_tor1;     h_tor1.set_params();
        hashfn::TornadoTab32D2 h_tor2;     h_tor2.set_params();
        hashfn::TornadoTab32D3 h_tor3;     h_tor3.set_params();
        hashfn::TornadoTab32D4 h_tor4;     h_tor4.set_params();

        auto run_one = [&](int idx) -> double {
            sketch::BottomK bk(K);
            switch (idx) {
            case IDX_MS:       for (auto x: keys) bk.push(h_ms.hash(x)); break;
            case IDX_STAB:     for (auto x: keys) bk.push(h_stab.hash(x)); break;
            case IDX_TOR_D1:   for (auto x: keys) bk.push(h_tor1.hash(x)); break;
            case IDX_TOR_D2:   for (auto x: keys) bk.push(h_tor2.hash(x)); break;
            case IDX_TOR_D3:   for (auto x: keys) bk.push(h_tor3.hash(x)); break;
            case IDX_TOR_D4:   for (auto x: keys) bk.push(h_tor4.hash(x)); break;
            case IDX_RAPID32:  for (auto x: keys) { std::uint32_t hv = h_rapid.hash(&x, sizeof(x)); bk.push(hv); } break;
            }
            const double est = bk.estimate();
            return (est - Dtrue) / Dtrue;
        };

        for (int f = 0; f < NUM_FUNCS; ++f) {
            const double relerr = run_one(f);
            out << NAMES[f] << "," << (r+1) << "," << relerr << "\n";
        }

        if (((r+1) % 1000) == 0 || (r+1)==R) {
            std::cout << "  rep " << (r+1) << " / " << R
                      << "  (" << (100.0 * double(r+1)/double(R)) << "%)\r" << std::flush;
        }
    }
    std::cout << "\nDone.\n";
    return 0;
}
