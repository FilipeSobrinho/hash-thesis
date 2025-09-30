// bench_ms_a2.cpp
// Measure pure hashing speed of hashfn::MS on dataset A2 (32-bit keys).
// No sketches, no parallelism. Reports hashes/sec and ns/hash.
//
// CLI:
//   --loops L     repeat the full pass L times (default 10)
//   --seedA S     fix 'a' (64-bit)  [optional]
//   --seedB S     fix 'b' (64-bit)  [optional]
//   --help
//
// Build:
//   add_executable(bench_ms_a2 src/bench_ms_a2.cpp)
//   target_include_directories(bench_ms_a2 PRIVATE ${CMAKE_SOURCE_DIR}/include)
//
// Run (example):
//   build\win-release-vs\Release\bench_ms_a2.exe --loops 50

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include <iomanip>

#include "core/a2.hpp"   // expects A2 with contiguous 4-byte items (N items)
#include "hash/ms.hpp"       // hashfn::MS  (32-bit input -> 32-bit output)
#include "core/randomgen.hpp"

static inline std::uint32_t load_le_u32(const std::uint8_t* p) {
    return (std::uint32_t)p[0]
        | ((std::uint32_t)p[1] << 8)
        | ((std::uint32_t)p[2] << 16)
        | ((std::uint32_t)p[3] << 24);
}

int main(int argc, char** argv) {
    try {
        // ---- options ----
        std::size_t loops = 10000;
        bool haveA = false, haveB = false;
        std::uint64_t A = 0, B = 0;

        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("missing value for " + a); };
            if (a == "--loops")  loops = std::stoull(next());
            else if (a == "--seedA") { A = std::stoull(next()); haveA = true; }
            else if (a == "--seedB") { B = std::stoull(next()); haveB = true; }
            else if (a == "--help" || a == "-h") {
                std::cout << "Usage: bench_ms_a2 [--loops L] [--seedA u64] [--seedB u64]\n";
                return 0;
            }
        }

        // ---- dataset ----
        datasets::A2 ds;                         // must expose buffer() with 4*N bytes, and size() = N
        const auto& buf = ds.buffer();
        const std::size_t N = ds.size();
        if (buf.size() != N * 4) {
            std::cerr << "A2: expected contiguous 4-byte items; got buffer=" << buf.size()
                << " bytes for N=" << N << "\n";
            return 2;
        }
        std::cout << "A2 items: " << N << " (4 bytes each)\n";

        // ---- hash function ----
        hashfn::MS ms;
        if (!haveA) A = rng::get_u64();
        if (!haveB) B = rng::get_u64();
        ms.set_params(A, B);
        std::cout << "MS params: a=" << A << " (odd enforced), b=" << B << "\n";

        // ---- warm-up (one pass) ----
        volatile std::uint32_t sink = 0;
        for (std::size_t i = 0; i < N; ++i) {
            const std::uint32_t x = load_le_u32(buf.data() + (i * 4));
            sink ^= ms.hash(x);
        }

        // ---- timed passes ----
        const auto t0 = std::chrono::steady_clock::now();
        for (std::size_t L = 0; L < loops; ++L) {
            for (std::size_t i = 0; i < N; ++i) {
                const std::uint32_t x = load_le_u32(buf.data() + (i * 4));
                sink ^= ms.hash(x);
            }
        }
        const auto t1 = std::chrono::steady_clock::now();
        const double seconds = std::chrono::duration<double>(t1 - t0).count();
        const std::size_t total_hashes = N * loops;

        // ---- report ----
        const double hps = total_hashes / seconds;
        const double nsph = (seconds * 1e9) / double(total_hashes);
        const double mhps = hps / 1e6;

        std::cout << std::fixed << std::setprecision(3);
        std::cout << "Loops: " << loops << "\n";
        std::cout << "Total hashes: " << total_hashes << "\n";
        std::cout << "Elapsed: " << seconds << " s\n";
        std::cout << "Throughput: " << mhps << " Mhash/s\n";
        std::cout << "Latency: " << nsph << " ns/hash\n";
        // Prevent optimizing-away:
        std::cout << "Checksum (ignore): " << std::hex << sink << std::dec << "\n";

        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
}
