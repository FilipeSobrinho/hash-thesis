// accuracy_oph_all_a1.cpp
// Multi-core OPH accuracy on A1 50/50 split (SAME DATASET FOR ALL REPS).
// CSV: function,rep,relerr   where relerr = (J_est - J_true)/J_true
//
// Hashers tested:
//   - MultShift (MS): 32-bit key -> 32-bit
//   - SimpleTab32: seedless (rng::get_u64()) under a mutex
//   - TornadoTab32D4: seedless per row (rng::get_u64()) under a mutex
//   - RapidHash32: top 32 bits of RapidHash; called with (ptr,len)
//
// Dataset:
//   - datasets::A1Split(N, split_seed) built ONCE; reused for every repetition.
//     We extract all 4B little-endian keys into A_keys and B_keys vectors once.
//
// Defaults: ITEMS=1e6, K=200 bins, R=50k reps (heavy!), split-seed=0xC0FFEE

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

#include "sketch/oph.hpp"
#include "core/randomgen.hpp"
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "core/a1.hpp"   // A1Split (ptr,len keys, 4 bytes LE)

static inline std::uint32_t load_le_u32(const void* p) {
    const std::uint8_t* b = static_cast<const std::uint8_t*>(p);
    return (std::uint32_t)b[0] | (std::uint32_t(b[1]) << 8) |
        (std::uint32_t(b[2]) << 16) | (std::uint32_t(b[3]) << 24);
}

// True Jaccard from two vectors of keys (distinct on key value).
static double jaccard_true_from_vectors(const std::vector<std::uint32_t>& A_keys,
    const std::vector<std::uint32_t>& B_keys) {
    std::unordered_set<std::uint32_t> Aset; Aset.reserve(A_keys.size());
    std::unordered_set<std::uint32_t> Bset; Bset.reserve(B_keys.size());
    for (auto k : A_keys) Aset.insert(k);
    for (auto k : B_keys) Bset.insert(k);
    // Intersection
    std::size_t inter = 0;
    if (Aset.size() < Bset.size()) {
        for (auto k : Aset) if (Bset.find(k) != Bset.end()) ++inter;
    }
    else {
        for (auto k : Bset) if (Aset.find(k) != Aset.end()) ++inter;
    }
    const std::size_t uni = Aset.size() + Bset.size() - inter;
    return uni ? double(inter) / double(uni) : 1.0;
}

int main(int argc, char** argv) {
    std::size_t ITEMS = 500'000;       // total stream items
    std::size_t K = 200;             // OPH bins
    std::size_t R = 50'000;          // repetitions
    std::uint64_t split_seed = 0xC0FFEEull; // SAME dataset across reps
    std::string outfile = "oph_all_relerr.csv";
    unsigned threads = std::thread::hardware_concurrency();
    if (!threads) threads = 4;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + arg); };
        if (arg == "--items")       ITEMS = std::stoull(next());
        else if (arg == "--K")      K = std::stoull(next());
        else if (arg == "--R")      R = std::stoull(next());
        else if (arg == "--out")    outfile = next();
        else if (arg == "--threads") threads = static_cast<unsigned>(std::stoul(next()));
        else if (arg == "--split-seed") split_seed = std::stoull(next());
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: accuracy_oph_all_a1 "
                "--items 1000000 --K 200 --R 50000 "
                "--out file.csv --threads N --split-seed 0xC0FFEE\n";
            return 0;
        }
    }

    std::cout << "OPH accuracy (ALL) on A1 50/50 split — SAME dataset per all reps\n"
        << "  items=" << ITEMS << "  K(bins)=" << K << "  R=" << R
        << "  threads=" << threads << "  split-seed=0x"
        << std::hex << split_seed << std::dec << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // ---------- Build ONE A1Split dataset and extract keys once ----------
    datasets::A1Split split(ITEMS, split_seed);

    std::vector<std::uint32_t> A_keys, B_keys;
    A_keys.reserve(ITEMS / 2 + 1024);
    B_keys.reserve(ITEMS / 2 + 1024);

    { // group A
        auto stA = split.make_streamA();
        const void* p; std::size_t len;
        while (stA.next(p, len)) A_keys.push_back(load_le_u32(p));
    }
    { // group B
        auto stB = split.make_streamB();
        const void* p; std::size_t len;
        while (stB.next(p, len)) B_keys.push_back(load_le_u32(p));
    }

    const double J_true = jaccard_true_from_vectors(A_keys, B_keys);
    const double denom = (J_true > 0.0 ? J_true : 1.0);

    // ---------- Hashers list & per-rep seeds (MS, Rapid only) ----------
    enum { IDX_MS, IDX_STAB, IDX_TORNADO_D4, IDX_RAPID32, NUM_FUNCS };
    const char* NAMES[NUM_FUNCS] = { "MultShift", "SimpleTab", "TornadoD4", "RapidHash32" };

    struct RepParams { std::uint64_t ms_a, ms_b, rapid_seed; };
    std::vector<RepParams> params(R);
    for (std::size_t r = 0; r < R; ++r) {
        params[r].ms_a = rng::get_u64();
        params[r].ms_b = rng::get_u64();
        params[r].rapid_seed = rng::get_u64();
    }

    // ---------- Parallel over repetitions ----------
    std::mutex file_mtx;        // final file writes
    std::mutex rng_mtx;         // seedless tabulation param gen (uses rng)
    std::mutex cout_mtx;        // progress prints
    std::atomic<std::size_t> done{ 0 };
    constexpr std::size_t PROG_STEP = 1000;

    auto worker = [&](unsigned tid) {
        std::ostringstream buf;

        for (std::size_t r = tid; r < R; r += threads) {
            // Per-rep hashers
            hashfn::MS h_ms; h_ms.set_params(params[r].ms_a, params[r].ms_b);

            rapid::RapidHash32 h_rapid;
            h_rapid.set_params(params[r].rapid_seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

            hashfn::SimpleTab32    h_stab;
            hashfn::TornadoTab32D4 h_tor4;
            { // guard RNG inside seedless tabulation param generation
                std::lock_guard<std::mutex> lk(rng_mtx);
                h_stab.set_params();   // seedless (Poly32 via rng::get_u64())
                h_tor4.set_params();   // seedless per-row (Poly32 via rng::get_u64())
            }

            // OPH sketches for each function (A and B)
            sketch::OPH S_A[NUM_FUNCS] = { sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K),
                                           sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K) };
            sketch::OPH S_B[NUM_FUNCS] = { sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K),
                                           sketch::OPH((uint32_t)K), sketch::OPH((uint32_t)K) };

            // Hash & push using the SAME dataset keys every rep
            // Group A
            for (auto key : A_keys) {
                std::uint32_t hv;
                hv = h_ms.hash(key);                  S_A[IDX_MS].push(hv);
                hv = h_stab.hash(key);                S_A[IDX_STAB].push(hv);
                hv = h_tor4.hash(key);                S_A[IDX_TORNADO_D4].push(hv);
                hv = h_rapid.hash(&key, sizeof(key)); S_A[IDX_RAPID32].push(hv);
            }
            // Group B
            for (auto key : B_keys) {
                std::uint32_t hv;
                hv = h_ms.hash(key);                  S_B[IDX_MS].push(hv);
                hv = h_stab.hash(key);                S_B[IDX_STAB].push(hv);
                hv = h_tor4.hash(key);                S_B[IDX_TORNADO_D4].push(hv);
                hv = h_rapid.hash(&key, sizeof(key)); S_B[IDX_RAPID32].push(hv);
            }

            // Emit relative error for each function
            for (int f = 0; f < NUM_FUNCS; ++f) {
                const double J_est = sketch::jaccard(S_A[f], S_B[f]);
                const double relerr = (J_est - J_true) / denom;
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

        // flush this thread’s results (RAII)
        { std::lock_guard<std::mutex> fl(file_mtx); out << buf.str(); }
        };

    std::vector<std::thread> pool; pool.reserve(threads);
    for (unsigned t = 0; t < threads; ++t) pool.emplace_back(worker, t);
    for (auto& th : pool) th.join();

    { std::lock_guard<std::mutex> io(cout_mtx); std::cout << "\n"; }
    std::cout << "Done.\n";
    return 0;
}
