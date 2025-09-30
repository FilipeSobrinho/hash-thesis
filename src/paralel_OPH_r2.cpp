// paralel_OPH_r2.cpp
// OPH accuracy on R2 (first 100k words), SAME dataset for all repetitions.
// Split: first half vs second half of that 100k slice (position-based halves).
// Output CSV: function,rep,relerr   where relerr = (J_est - J_true) / J_true.
//
// Sketch API (as in R1):
//   - OPH::push(uint32_t hv)   // store min hv per bin
//   - jaccard(OPH,OPH)         // compare two OPH sketches   :contentReference[oaicite:3]{index=3}
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
#include "hash/msvec.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "core/r2.hpp"

// Byte-view for set ops
struct View {
    const std::uint8_t* p;
    std::uint32_t len;
    bool operator==(const View& o) const noexcept { return len == o.len && std::memcmp(p, o.p, len) == 0; }
};
struct ViewHash {
    std::size_t operator()(const View& v) const noexcept {
        std::uint32_t h = 2166136261u;
        for (std::uint32_t i = 0; i < v.len; ++i) { h ^= v.p[i]; h *= 16777619u; }
        return (std::size_t)h;
    }
};

static double jaccard_true_from_halves(const datasets::R2& base) {
    const auto& buf = base.buffer();
    const auto& index = base.index();
    const std::size_t N = index.size();
    const std::size_t mid = N / 2;

    std::unordered_set<View, ViewHash> A, B;
    A.reserve(mid); B.reserve(N - mid);

    for (std::size_t i = 0; i < N; ++i) {
        const auto [off, len] = index[i];
        View v{ buf.data() + off, (std::uint32_t)len };
        if (i < mid) A.insert(v); else B.insert(v);
    }
    std::size_t inter = 0;
    if (A.size() < B.size()) { for (auto& v : A) inter += B.count(v); }
    else { for (auto& v : B) inter += A.count(v); }
    const std::size_t uni = A.size() + B.size() - inter;
    return uni ? double(inter) / double(uni) : 1.0;
}

int main(int argc, char** argv) {
    std::size_t K = 200;           // bins
    std::size_t R = 50'000;
    std::string outfile = "oph_r2_relerr.csv";
    unsigned threads = std::thread::hardware_concurrency(); if (!threads) threads = 4;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + a); };
        if (a == "--K") K = std::stoull(next());
        else if (a == "--R") R = std::stoull(next());
        else if (a == "--out") outfile = next();
        else if (a == "--threads") threads = (unsigned)std::stoul(next());
        else if (a == "--help" || a == "-h") {
            std::cout << "Usage: paralel_OPH_r2 --K 200 --R 50000 --out oph_r2.csv --threads N\n";
            return 0;
        }
    }

    std::cout << "OPH on R2 (first 100k words), same dataset for all reps\n"
        << "  K=" << K << "  R=" << R << "  threads=" << threads << "\n"
        << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary); if (!out) { std::cerr << "Cannot open " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    // Build the dataset once
    datasets::R2 base;
    const auto& buf = base.buffer();
    const auto& index = base.index();
    const std::size_t ITEMS = index.size();
    if (!ITEMS) { std::cerr << "R2: empty dataset\n"; return 2; }

    // First-half vs second-half split (R2 spec)
    const std::size_t mid = ITEMS / 2;

    // Compute J_true on DISTINCT words of the halves
    const double J_true = jaccard_true_from_halves(base);
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
        std::ostringstream bufout;

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

            // Feed halves: A = [0, mid), B = [mid, N)
            // Group A
            for (std::size_t i = 0; i < mid; ++i) {
                const auto [off, len] = index[i];
                const void* p = buf.data() + off;
                SA[IDX_MSVEC].push(msvec.hash(p, len));
                SA[IDX_TABMS].push(tabms.hash(p, len));
                SA[IDX_TOR4].push(tor4.hash(p, len));
                SA[IDX_RAPID32].push(rh32.hash(p, len));
            }
            // Group B
            for (std::size_t i = mid; i < ITEMS; ++i) {
                const auto [off, len] = index[i];
                const void* p = buf.data() + off;
                SB[IDX_MSVEC].push(msvec.hash(p, len));
                SB[IDX_TABMS].push(tabms.hash(p, len));
                SB[IDX_TOR4].push(tor4.hash(p, len));
                SB[IDX_RAPID32].push(rh32.hash(p, len));
            }

            for (int f = 0; f < NUM_FUNCS; ++f) {
                const double J_est = sketch::jaccard(SA[f], SB[f]);
                const double rel = (J_est - J_true) / denom;
                bufout << NAMES[f] << "," << (r + 1) << "," << rel << "\n";
            }

            // progress
            std::size_t n = done.fetch_add(1, std::memory_order_relaxed) + 1;
            if ((n % PROG_STEP) == 0 || n == R) {
                std::lock_guard<std::mutex> io(cout_mtx);
                std::cout << "  rep " << n << " / " << R
                    << " (" << (100.0 * double(n) / double(R)) << "%)\r" << std::flush;
            }
        }

        std::lock_guard<std::mutex> g(file_mtx);
        out << bufout.str();
        };

    std::vector<std::thread> pool; pool.reserve(threads);
    for (unsigned t = 0; t < threads; ++t) pool.emplace_back(worker, t);
    for (auto& th : pool) th.join();

    { std::lock_guard<std::mutex> io(cout_mtx); std::cout << "\n"; }
    std::cout << "Done.\n";
    return 0;
}
