// paralel_BK_r1.cpp
// Multi-core Bottom-k accuracy for MSVec-based hash functions (plus RapidHash32) on R1 dataset.
// CSV: function,rep,relerr   where relerr = (est - Dtrue) / Dtrue
//
// Hashers (all 32-bit outputs):
//   - MSVec: any-length -> 32-bit (high 32 of 64-bit accumulator)
//   - TabOnMSVec: MSVec prehash -> SimpleTab32
//   - TornadoOnMSVecD4: MSVec prehash -> Tornado (D=4)
//   - RapidHash32: top-32 bits of 64-bit RapidHash; called with (ptr,len)
//
// Dataset:
//   - datasets::R1(): 500k items, each 20 bytes (SHA-1), sampled w/ replacement from first 250k lines.
//     We materialize once, compute Dtrue once on 20-byte keys, and reuse across repetitions.
//
// CLI:
//   --k K         bottom-k size (default 24500)
//   --R R         repetitions (default 1000)
//   --out FILE    output CSV (default "bottomk_r1_relerr.csv")
//   --threads N   thread count (default: HW concurrency)
//   --help        usage

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

#include "sketch/bottomk.hpp"
#include "core/randomgen.hpp"       // rng::get_u64()
#include "hash/msvec.hpp"           // hashfn::MSVec (varlen -> 32-bit) [MSVEC_NUM_COEFFS]
#include "hash/simpletab32.hpp"     // hashfn::TabOnMSVec (MSVec -> SimpleTab32)
#include "hash/tornado32.hpp"       // hashfn::TornadoOnMSVecD4 (MSVec -> Tornado)
#include "hash/rapidhash.h"         // rapid::RapidHash32 (varlen)
#include "core/r1.hpp"              // datasets::R1 (20-byte items)   <-- adjust include path if needed

// Hash for 20-byte keys used to compute Dtrue
struct Key20 {
    std::array<std::uint8_t, 20> b{};
    bool operator==(const Key20& o) const noexcept { return std::memcmp(b.data(), o.b.data(), 20) == 0; }
};
struct Key20Hash {
    std::size_t operator()(const Key20& k) const noexcept {
        // simple FNV-1a fold to size_t
        std::uint32_t h = 2166136261u;
        for (auto x : k.b) { h ^= x; h *= 16777619u; }
        return (std::size_t)h;
    }
};

int main(int argc, char** argv) {
    // Defaults
    std::size_t K = 24'500;            // bottom-k size
    std::size_t R = 1'000;             // repetitions
    std::string outfile = "bottomk_r1_relerr.csv";
    unsigned threads = std::thread::hardware_concurrency();
    if (threads == 0) threads = 4;

    // Parse CLI
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 < argc) return std::string(argv[++i]);
            throw std::runtime_error("Missing value for " + arg);
            };
        if (arg == "--k") K = std::stoull(next());
        else if (arg == "--R") R = std::stoull(next());
        else if (arg == "--out") outfile = next();
        else if (arg == "--threads") threads = static_cast<unsigned>(std::stoul(next()));
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: paralel_BK_r1 [--k 24500] [--R 1000] "
                "[--out file.csv] [--threads N]\n";
            return 0;
        }
    }

    std::cout << "Bottom-k accuracy (R1 dataset)\n"
        << "  k=" << K << "  R=" << R << "  threads=" << threads << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed);
    out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // ---------- Build ONE R1 dataset and extract as raw 20B items ----------
    datasets::R1 base; // 500k sample-with-replacement from first 250k lines
    const auto& raw = base.buffer();              // contiguous bytes
    const std::size_t ITEMS = base.size();        // 500k expected
    if (raw.size() != ITEMS * 20) {
        std::cerr << "R1 raw size mismatch\n";
        return 2;
    }

    // Compute true distinct count Dtrue once (on 20-byte keys)
    std::unordered_set<Key20, Key20Hash> uniq;
    uniq.reserve(ITEMS);
    for (std::size_t i = 0; i < ITEMS; ++i) {
        Key20 k;
        std::memcpy(k.b.data(), raw.data() + i * 20, 20);
        uniq.insert(k);
    }
    const double Dtrue = static_cast<double>(uniq.size());

    // Function list
    enum { IDX_MSVEC, IDX_TAB_ON_MSVEC, IDX_TOR_ON_MSVEC_D4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MSVec", "TabOnMSVec", "TornadoOnMSVecD4", "RapidHash32" };

    using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;

    // Pre-generate per-repetition params:
    //  - One MSVec coefficient vector per repetition (shared by MSVec/Tab/Tornado prehash)
    //  - One RapidHash seed per repetition
    struct RepParams { Coeffs coeffs; std::uint64_t rapid_seed; };
    std::vector<RepParams> params(R);
    for (std::size_t r = 0; r < R; ++r) {
        for (std::size_t i = 0; i < MSVEC_NUM_COEFFS; ++i) params[r].coeffs[i] = rng::get_u64();
        params[r].rapid_seed = rng::get_u64();
    }

    // Concurrency primitives
    std::mutex file_mtx;   // protect output file writes
    std::mutex cout_mtx;   // protect progress printing
    std::atomic<std::size_t> done{ 0 };
    constexpr std::size_t PROG_STEP = 1000;

    auto run_one = [&](int idx,
        const hashfn::MSVec& msvec,
        const hashfn::TabOnMSVec& tabms,
        const hashfn::TornadoOnMSVecD4& tor4,
        const rapid::RapidHash32& rh32) -> double
        {
            sketch::BottomK bk(K);

            switch (idx) {
            case IDX_MSVEC:
                for (std::size_t i = 0; i < ITEMS; ++i) {
                    const void* p = raw.data() + i * 20;
                    std::uint32_t hv = msvec.hash(p, 20);
                    bk.push(hv);
                }
                break;

            case IDX_TAB_ON_MSVEC:
                for (std::size_t i = 0; i < ITEMS; ++i) {
                    const void* p = raw.data() + i * 20;
                    std::uint32_t hv = tabms.hash(p, 20);
                    bk.push(hv);
                }
                break;

            case IDX_TOR_ON_MSVEC_D4:
                for (std::size_t i = 0; i < ITEMS; ++i) {
                    const void* p = raw.data() + i * 20;
                    std::uint32_t hv = tor4.hash(p, 20);
                    bk.push(hv);
                }
                break;

            case IDX_RAPID32:
                for (std::size_t i = 0; i < ITEMS; ++i) {
                    const void* p = raw.data() + i * 20;
                    std::uint32_t hv = rh32.hash(p, 20);
                    bk.push(hv);
                }
                break;
            }

            const double est = bk.estimate();
            return (est - Dtrue) / Dtrue;
        };

    auto worker = [&](unsigned tid) {
        std::ostringstream buf;
        for (std::size_t r = tid; r < R; r += threads) {
            // Build per-repetition hashers
            hashfn::MSVec msvec;                 msvec.set_params(params[r].coeffs, true);
            hashfn::TabOnMSVec tabms;            tabms.set_params(params[r].coeffs, true);
            hashfn::TornadoOnMSVecD4 tor4;       tor4.set_params(params[r].coeffs, true);

            rapid::RapidHash32 rh32;
            rh32.set_params(params[r].rapid_seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

            for (int f = 0; f < NUM_FUNCS; ++f) {
                const double relerr = run_one(f, msvec, tabms, tor4, rh32);
                buf << NAMES[f] << "," << (r + 1) << "," << relerr << "\n";
            }

            // Progress (every 1000 reps, and at the end)
            std::size_t n = done.fetch_add(1, std::memory_order_relaxed) + 1;
            if ((n % PROG_STEP) == 0 || n == R) {
                std::lock_guard<std::mutex> io(cout_mtx);
                std::cout << "  rep " << n << " / " << R
                    << "  (" << (100.0 * double(n) / double(R)) << "%)\r"
                    << std::flush;
            }
        }
        // Flush this thread's chunk once
        std::lock_guard<std::mutex> fl(file_mtx);
        out << buf.str();
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
