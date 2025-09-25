// accuracy_bottomk_all_a1.cpp
// Multi-core Bottom-k accuracy for multiple hash functions on A1 dataset.
// CSV: function,rep,relerr   where relerr = (est - Dtrue) / Dtrue
//
// Hashers:
//   - MultShift (MS): 32-bit key -> 32-bit
//   - SimpleTab32: seedless table (rng::get_u64()) under a mutex
//   - TornadoTab32D4: seedless per-row (rng::get_u64()) under a mutex
//   - RapidHash32: 32-bit = top-32 of 64-bit RapidHash; called with (ptr,len)
//
// Dataset:
//   - datasets::A1(ITEMS): fully materialized stream of 4B LE keys.
//     We extract all keys once into a vector<uint32_t>, compute Dtrue once,
//     and reuse the same dataset for all repetitions.
//
// CLI (compatible with prior file):
//   --items N   (preferred)    total items in A1 stream (default 500000)
//   --D N       (alias for --items)
//   --k K       bottom-k size (default 24500)
//   --R R       repetitions (default 50000)
//   --out FILE  output CSV
//   --threads N thread count

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
#include <unordered_set>
#include <vector>

#include "sketch/bottomk.hpp"
#include "core/randomgen.hpp"       // rng::get_u64()
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"     // seedless set_params()
#include "hash/tornado32.hpp"       // seedless per-row set_params()
#include "hash/rapidhash.h"         // rapid::RapidHash32
#include "core/a1.hpp"          // datasets::A1

// Load 32-bit LE
static inline std::uint32_t load_le_u32(const void* p) {
    const std::uint8_t* b = static_cast<const std::uint8_t*>(p);
    return (std::uint32_t)b[0] | (std::uint32_t(b[1]) << 8)
        | (std::uint32_t(b[2]) << 16) | (std::uint32_t(b[3]) << 24);
}

int main(int argc, char** argv) {
    // Defaults
    std::size_t ITEMS = 500'000;   // total items in A1 stream  (alias: --D)
    std::size_t K = 24'500;    // bottom-k size
    std::size_t R = 1'000;    // repetitions
    std::string outfile = "bottomk_all_relerr.csv";
    unsigned threads = std::thread::hardware_concurrency();
    if (threads == 0) threads = 4;

    // Parse CLI
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 < argc) return std::string(argv[++i]);
            throw std::runtime_error("Missing value for " + arg);
            };
        if (arg == "--items") ITEMS = std::stoull(next());
        else if (arg == "--D") ITEMS = std::stoull(next()); // backward-compat alias
        else if (arg == "--k") K = std::stoull(next());
        else if (arg == "--R") R = std::stoull(next());
        else if (arg == "--out") outfile = next();
        else if (arg == "--threads") threads = static_cast<unsigned>(std::stoul(next()));
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: accuracy_bottomk_all_a1 "
                "[--items 500000] [--k 24500] [--R 50000] "
                "[--out file.csv] [--threads N]\n";
            return 0;
        }
    }

    std::cout << "Bottom-k accuracy (ALL) on A1 dataset\n"
        << "  items=" << ITEMS << "  k=" << K << "  R=" << R
        << "  threads=" << threads << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed);
    out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // ---------- Build ONE A1 dataset and extract all keys once ----------
    datasets::A1 base(ITEMS);
    std::vector<std::uint32_t> keys; keys.reserve(ITEMS);

    {
        auto st = base.make_stream();     // pointer+length, 4B per record
        const void* p; std::size_t len;
        while (st.next(p, len)) keys.push_back(load_le_u32(p));
    }

    // Compute true distinct count Dtrue once (from keys)
    std::unordered_set<std::uint32_t> uniq;
    uniq.reserve(keys.size());
    for (auto v : keys) uniq.insert(v);
    const double Dtrue = static_cast<double>(uniq.size());

    // Function list
    enum { IDX_MS, IDX_STAB, IDX_TORNADO_D4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MultShift", "SimpleTab", "TornadoD4", "RapidHash32" };

    // Pre-generate seeds for MS & Rapid only (tabulation stays seedless)
    struct RepParams { std::uint64_t ms_a, ms_b, rapid_seed; };
    std::vector<RepParams> params(R);
    for (std::size_t r = 0; r < R; ++r) {
        params[r].ms_a = rng::get_u64();
        params[r].ms_b = rng::get_u64();
        params[r].rapid_seed = rng::get_u64();
    }

    // Concurrency primitives
    std::mutex file_mtx;   // protect final file writes
    std::mutex rng_mtx;    // protect RNG usage inside seedless set_params()
    std::mutex cout_mtx;   // protect progress printing
    std::atomic<std::size_t> done{ 0 };
    constexpr std::size_t PROG_STEP = 1000;

    auto worker = [&](unsigned tid) {
        std::ostringstream buf;
        for (std::size_t r = tid; r < R; r += threads) {
            // Hashers with per-repetition params
            hashfn::MS h_ms; h_ms.set_params(params[r].ms_a, params[r].ms_b);

            rapid::RapidHash32 h_rapid;
            h_rapid.set_params(params[r].rapid_seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

            // Seedless tabulation (mutex-guarded RNG use)
            hashfn::SimpleTab32    h_stab;
            hashfn::TornadoTab32D4 h_tor4;
            {
                std::lock_guard<std::mutex> lk(rng_mtx);
                h_stab.set_params();   // seedless (Poly32 via rng::get_u64())
                h_tor4.set_params();   // seedless per-row (Poly32 via rng::get_u64())
            }

            auto run_one = [&](int idx) -> double {
                sketch::BottomK bk(K);
                switch (idx) {
                case IDX_MS:
                    for (auto x : keys) { bk.push(h_ms.hash(x)); }
                    break;
                case IDX_STAB:
                    for (auto x : keys) { bk.push(h_stab.hash(x)); }
                    break;
                case IDX_TORNADO_D4:
                    for (auto x : keys) { bk.push(h_tor4.hash(x)); }
                    break;
                case IDX_RAPID32:
                    for (auto x : keys) { std::uint32_t hv = h_rapid.hash(&x, sizeof(x)); bk.push(hv); }
                    break;
                }
                const double est = bk.estimate();
                return (est - Dtrue) / Dtrue;
                };

            for (int f = 0; f < NUM_FUNCS; ++f) {
                const double relerr = run_one(f);
                buf << NAMES[f] << "," << (r + 1) << "," << relerr << "\n";
            }

            // Progress ( every 1000 reps)
            std::size_t n = done.fetch_add(1, std::memory_order_relaxed) + 1;
            if ((n % PROG_STEP) == 0 || n == R) {
                std::lock_guard<std::mutex> io(cout_mtx);
                std::cout << "  rep " << n << " / " << R
                    << "  (" << (100.0 * double(n) / double(R)) << "%)\r"
                    << std::flush;
            }
        }
        // Flush this thread's chunk
        { std::lock_guard<std::mutex> fl(file_mtx); out << buf.str(); }
        };

    // Launch pool
    std::vector<std::thread> pool; pool.reserve(threads);
    for (unsigned t = 0; t < threads; ++t) pool.emplace_back(worker, t);
    for (auto& th : pool) th.join();

    // Finish progress line
    { std::lock_guard<std::mutex> io(cout_mtx); std::cout << "\n"; }

    std::cout << "Done.\n";

    return 0;
}
