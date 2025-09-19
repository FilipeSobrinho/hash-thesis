// paralel_CMS_a1.cpp
// Multi-core Count–Min Sketch accuracy for multiple hash families on A1 dataset.
// CSV: function,rep,relerr   where relerr = mean_{keys with true>0} ( (est-true)/true )
//
// Families:
//   - MultShift (MS)         : per-row (a,b) seeds
//   - SimpleTab32            : per-row set_params()   (NO parameters; uses internal RNG)
//   - TornadoTab32D4         : per-row set_params()   (NO parameters; uses internal RNG)
//   - RapidHash32 (adapter)  : per-row seed
// Each repetition: all rows use the SAME family; rows get different params.
//
// Dataset: datasets/a1.hpp (fully materialized A1 stream). Same dataset for all reps.
// Defaults: ITEMS=500000, WIDTH=32768, DEPTH=3, R=50000 (heavy)

#include <algorithm>
#include <atomic>
#include <cstdint>
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
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"     // now: set_params() with NO args
#include "hash/tornado32.hpp"       // now: set_params() with NO args
#include "hash/rapidhash.h"
#include "core/a1.hpp"          // adjust include path if your tree differs

// ---- helpers ----
static inline std::uint32_t load_le_u32(const void* p) {
    const std::uint8_t* b = static_cast<const std::uint8_t*>(p);
    return (std::uint32_t)b[0] | (std::uint32_t(b[1]) << 8)
        | (std::uint32_t(b[2]) << 16) | (std::uint32_t(b[3]) << 24);
}

// RapidHash row adapter (hash(uint32_t) API for CMS rows)
struct RapidRow32 {
    rapid::RapidHash32 h;
    void set_seed(std::uint64_t seed) {
        // reuse global secrets; vary only the seed across rows
        h.set_params(seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);
    }
    std::uint32_t hash(std::uint32_t key) const {
        return h.hash(&key, sizeof(key));
    }
};

int main(int argc, char** argv) {
    // ---- defaults ----
    std::size_t ITEMS = 500'000;
    std::size_t WIDTH = 32'768;  // 32768
    std::size_t DEPTH = 3;
    std::size_t R = 50'000;
    std::string outfile = "cms_all_relerr.csv";
    unsigned threads = std::thread::hardware_concurrency();
    if (!threads) threads = 4;

    // ---- CLI ----
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + arg); };
        if (arg == "--items" || arg == "--D") ITEMS = std::stoull(next());
        else if (arg == "--width")  WIDTH = std::stoull(next());
        else if (arg == "--depth")  DEPTH = std::stoull(next());
        else if (arg == "--R")      R = std::stoull(next());
        else if (arg == "--out")    outfile = next();
        else if (arg == "--threads") threads = static_cast<unsigned>(std::stoul(next()));
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: paralel_CMS_a1 "
                "--items 500000 --width 32768 --depth 3 --R 50000 "
                "--out cms_all_relerr.csv --threads N\n";
            return 0;
        }
    }

    std::cout << "CMS accuracy (ALL) on A1\n"
        << "  items=" << ITEMS
        << "  width=" << WIDTH
        << "  depth=" << DEPTH
        << "  R=" << R
        << "  threads=" << threads << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // ---------- Build ONE A1 dataset and extract all keys once ----------
    datasets::A1 base(ITEMS);
    std::vector<std::uint32_t> keys; keys.reserve(ITEMS);
    {
        auto st = base.make_stream();
        const void* p; std::size_t len;
        while (st.next(p, len)) keys.push_back(load_le_u32(p));
    }

    // ---------- True frequencies over distinct keys ----------
    std::unordered_map<std::uint32_t, std::uint32_t> freq;
    freq.reserve(keys.size() / 2 + 1024);
    for (auto v : keys) ++freq[v];

    std::vector<std::uint32_t> distinct;
    distinct.reserve(freq.size());
    for (auto& kv : freq) distinct.push_back(kv.first);

    // ---------- families ----------
    enum { IDX_MS, IDX_STAB, IDX_TORNADO_D4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MultShift", "SimpleTab", "TornadoD4", "RapidHash32" };

    // ---------- seeds per repetition and row (for MS & Rapid only) ----------
    struct RepSeeds {
        std::vector<std::pair<std::uint64_t, std::uint64_t>> row_ms_ab; // (a,b) per row for MS
        std::vector<std::uint64_t> row_rapid;                          // seed per row for Rapid
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

    // ---------- helpers ----------
    auto mean_relative_error = [&](const sketch::CountMin& cms) -> double {
        long double sum = 0.0L;
        std::size_t cnt = 0;
        for (auto k : distinct) {
            const std::uint32_t truef = freq.find(k)->second; // >0 by construction
            const std::uint32_t estf = cms.estimate(k);
            sum += (static_cast<long double>(estf) - static_cast<long double>(truef))
                / static_cast<long double>(truef);
            ++cnt;
        }
        return (cnt ? static_cast<double>(sum / cnt) : 0.0);
        };

    // ---------- parallel loop ----------
    std::mutex file_mtx;
    std::mutex cout_mtx;
    std::mutex rng_mtx; // guard seedless tabulation set_params() so rows differ deterministically
    std::atomic<std::size_t> done{ 0 };
    constexpr std::size_t PROG_STEP = 1000;

    auto worker = [&](unsigned tid) {
        std::ostringstream buf;

        for (std::size_t r = tid; r < R; r += threads) {

            auto run_family = [&](int family_idx) {
                sketch::CountMin cms(WIDTH, DEPTH);

                switch (family_idx) {
                case IDX_MS: {
                    std::vector<hashfn::MS> rows(DEPTH);
                    for (std::size_t d = 0; d < DEPTH; ++d) {
                        rows[d].set_params(seeds[r].row_ms_ab[d].first,
                            seeds[r].row_ms_ab[d].second);
                        cms.set_row(d, &rows[d]);
                    }
                    for (auto k : keys) cms.add(k, 1);
                    return mean_relative_error(cms);
                }
                case IDX_STAB: {
                    std::vector<hashfn::SimpleTab32> rows(DEPTH);
                    {   // Seedless, but ensure distinct tables per row in deterministic order
                        std::lock_guard<std::mutex> lk(rng_mtx);
                        for (std::size_t d = 0; d < DEPTH; ++d) rows[d].set_params(); // NO args
                    }
                    for (std::size_t d = 0; d < DEPTH; ++d) cms.set_row(d, &rows[d]);
                    for (auto k : keys) cms.add(k, 1);
                    return mean_relative_error(cms);
                }
                case IDX_TORNADO_D4: {
                    std::vector<hashfn::TornadoTab32D4> rows(DEPTH);
                    {   // Seedless, but ensure distinct tables per row in deterministic order
                        std::lock_guard<std::mutex> lk(rng_mtx);
                        for (std::size_t d = 0; d < DEPTH; ++d) rows[d].set_params(); // NO args
                    }
                    for (std::size_t d = 0; d < DEPTH; ++d) cms.set_row(d, &rows[d]);
                    for (auto k : keys) cms.add(k, 1);
                    return mean_relative_error(cms);
                }
                case IDX_RAPID32: {
                    std::vector<RapidRow32> rows(DEPTH);
                    for (std::size_t d = 0; d < DEPTH; ++d) {
                        rows[d].set_seed(seeds[r].row_rapid[d]);
                        cms.set_row(d, &rows[d]);
                    }
                    for (auto k : keys) cms.add(k, 1);
                    return mean_relative_error(cms);
                }
                }
                return 0.0; // unreachable
                };

            for (int f = 0; f < NUM_FUNCS; ++f) {
                const double relerr = run_family(f);
                buf << NAMES[f] << "," << (r + 1) << "," << relerr << "\n";
            }

            // progress ( every 1000 reps)
            std::size_t n = done.fetch_add(1, std::memory_order_relaxed) + 1;
            if ((n % PROG_STEP) == 0 || n == R) {
                std::lock_guard<std::mutex> io(cout_mtx);
                std::cout << "  rep " << n << " / " << R
                    << "  (" << (100.0 * double(n) / double(R)) << "%)\r"
                    << std::flush;
            }
        }

        { std::lock_guard<std::mutex> fl(file_mtx); out << buf.str(); }
        };

    std::vector<std::thread> pool; pool.reserve(threads);
    for (unsigned t = 0; t < threads; ++t) pool.emplace_back(worker, t);
    for (auto& th : pool) th.join();

    { std::lock_guard<std::mutex> io(cout_mtx); std::cout << "\n"; }
    std::cout << "Done.\n";
    return 0;
}
