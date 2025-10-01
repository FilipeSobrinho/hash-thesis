// serial_CM_a2.cpp
// Count–Min Sketch accuracy, single-threaded, A2 dataset.
// CSV: function,rep,relerr (mean relative error across distinct keys)
// Families: MS, SimpleTab, Tornado D1..D4, RapidHash32

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "sketch/countmin.hpp"
#include "core/randomgen.hpp"
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "core/a2.hpp"

static inline std::uint32_t load_le_u32(const void* p) {
    const std::uint8_t* b = static_cast<const std::uint8_t*>(p);
    return (std::uint32_t)b[0] | (std::uint32_t(b[1]) << 8)
        | (std::uint32_t(b[2]) << 16) | (std::uint32_t(b[3]) << 24);
}

struct RapidRow32 {
    rapid::RapidHash32 h;
    void set_seed(std::uint64_t seed) { h.set_params(seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]); }
    std::uint32_t hash(std::uint32_t key) const { return h.hash(&key, sizeof(key)); }
};

int main(int argc, char** argv) {
    std::size_t ITEMS = 500'000;
    std::size_t WIDTH = 32'768;
    std::size_t DEPTH = 3;
    std::size_t R = 50'000;
    std::string outfile = "cms_a2_relerr.csv";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + arg); };
        if (arg == "--items" || arg == "--D") ITEMS = std::stoull(next());
        else if (arg == "--width")  WIDTH = std::stoull(next());
        else if (arg == "--depth")  DEPTH = std::stoull(next());
        else if (arg == "--R")      R = std::stoull(next());
        else if (arg == "--out")    outfile = next();
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: serial_CM_a2 --items 500000 --width 32768 --depth 3 --R 1000 --out cms_a2_relerr.csv\n";
            return 0;
        }
    }

    std::cout << "CMS accuracy (A2) single-threaded\n"
              << "  items=" << ITEMS << "  width=" << WIDTH << "  depth=" << DEPTH << "  R=" << R << "\n"
              << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // Build dataset & frequencies
    datasets::A2 base;
    std::vector<std::uint32_t> keys; keys.reserve(ITEMS);
    {
        auto st = base.make_stream();
        const void* p; std::size_t len;
        while (st.next(p, len)) keys.push_back(load_le_u32(p));
    }
    std::unordered_map<std::uint32_t, std::uint32_t> freq;
    freq.reserve(keys.size()/2 + 1024);
    for (auto v : keys) ++freq[v];

    std::vector<std::uint32_t> distinct; distinct.reserve(freq.size());
    for (auto& kv : freq) distinct.push_back(kv.first);

    enum { IDX_MS, IDX_STAB, IDX_TOR_D1, IDX_TOR_D2, IDX_TOR_D3, IDX_TOR_D4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MultShift","SimpleTab","TornadoD1","TornadoD2","TornadoD3","TornadoD4","RapidHash32" };

    struct RepSeeds {
        std::vector<std::pair<std::uint64_t, std::uint64_t>> row_ms_ab; // length DEPTH
        std::vector<std::uint64_t> row_rapid;                           // length DEPTH
    };
    std::vector<RepSeeds> seeds(R);
    for (std::size_t r = 0; r < R; ++r) {
        seeds[r].row_ms_ab.resize(DEPTH);
        seeds[r].row_rapid.resize(DEPTH);
        for (std::size_t d = 0; d < DEPTH; ++d) {
            seeds[r].row_ms_ab[d] = { rng::get_u64(), rng::get_u64() };
            seeds[r].row_rapid[d] = rng::get_u64();
        }
    }

    auto mean_relative_error = [&](const sketch::CountMin& cms) -> double {
        long double sum = 0.0L; std::size_t cnt = 0;
        for (auto k : distinct) {
            const std::uint32_t truef = freq.find(k)->second;
            const std::uint32_t estf  = cms.estimate(k);
            sum += (static_cast<long double>(estf) - static_cast<long double>(truef)) / static_cast<long double>(truef);
            ++cnt;
        }
        return (cnt ? static_cast<double>(sum / cnt) : 0.0);
    };

    for (std::size_t r = 0; r < R; ++r) {
        auto run_family = [&](int family_idx) {
            sketch::CountMin cms(WIDTH, DEPTH);
            switch (family_idx) {
            case IDX_MS: {
                std::vector<hashfn::MS> rows(DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) { rows[d].set_params(seeds[r].row_ms_ab[d].first, seeds[r].row_ms_ab[d].second); cms.set_row(d, &rows[d]); }
                for (auto k : keys) cms.add(k, 1);
                return mean_relative_error(cms);
            }
            case IDX_STAB: {
                std::vector<hashfn::SimpleTab32> rows(DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) { rows[d].set_params(); cms.set_row(d, &rows[d]); }
                for (auto k : keys) cms.add(k, 1);
                return mean_relative_error(cms);
            }
            case IDX_TOR_D1: {
                std::vector<hashfn::TornadoTab32D1> rows(DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) { rows[d].set_params(); cms.set_row(d, &rows[d]); }
                for (auto k : keys) cms.add(k, 1);
                return mean_relative_error(cms);
            }
            case IDX_TOR_D2: {
                std::vector<hashfn::TornadoTab32D2> rows(DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) { rows[d].set_params(); cms.set_row(d, &rows[d]); }
                for (auto k : keys) cms.add(k, 1);
                return mean_relative_error(cms);
            }
            case IDX_TOR_D3: {
                std::vector<hashfn::TornadoTab32D3> rows(DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) { rows[d].set_params(); cms.set_row(d, &rows[d]); }
                for (auto k : keys) cms.add(k, 1);
                return mean_relative_error(cms);
            }
            case IDX_TOR_D4: {
                std::vector<hashfn::TornadoTab32D4> rows(DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) { rows[d].set_params(); cms.set_row(d, &rows[d]); }
                for (auto k : keys) cms.add(k, 1);
                return mean_relative_error(cms);
            }
            case IDX_RAPID32: {
                std::vector<RapidRow32> rows(DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) { rows[d].set_seed(seeds[r].row_rapid[d]); cms.set_row(d, &rows[d]); }
                for (auto k : keys) cms.add(k, 1);
                return mean_relative_error(cms);
            }
            }
            return 0.0;
        };

        for (int f = 0; f < NUM_FUNCS; ++f) {
            const double relerr = run_family(f);
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
