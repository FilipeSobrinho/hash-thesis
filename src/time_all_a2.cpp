// time_all_a2.cpp
// Hashing speed on A2 (32-bit keys) for:
// MS, SimpleTab32, Tornado32_D1..D4, RapidHash32.
// Always writes CSV (default: a2_speed.csv).
// CLI: --loops L  --out file.csv  --help
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <optional>
#include <fstream>
#include "core/a2.hpp"
#include "core/randomgen.hpp"
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"

static inline std::uint32_t load_le_u32(const std::uint8_t* p) {
    return (std::uint32_t)p[0] | ((std::uint32_t)p[1] << 8) |
           ((std::uint32_t)p[2] << 16) | ((std::uint32_t)p[3] << 24);
}

template <typename Body>
static std::pair<double, std::uint32_t> time_loops(std::size_t loops, Body&& body) {
    volatile std::uint32_t sink = 0; body(sink); // warmup
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t L = 0; L < loops; ++L) body(sink);
    const auto t1 = std::chrono::steady_clock::now();
    return { std::chrono::duration<double>(t1 - t0).count(), sink };
}

int main(int argc, char** argv) {
    try {
        std::size_t loops = 1000;
        std::string out_csv = "a2_speed.csv";
        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("missing value for " + a); };
            if (a == "--loops") loops = std::stoull(next());
            else if (a == "--out") out_csv = next();
            else if (a == "--help" || a == "-h") {
                std::cout << "Usage: time_all_a2 [--loops L] [--out file.csv]\n"; return 0;
            }
        }

        datasets::A2 ds;
        const auto& buf = ds.buffer();
        const std::size_t N = ds.size();
        if (buf.size() != N * 4) { std::cerr << "A2 contiguous 4B expected\n"; return 2; }
        std::cout << "A2 items: " << N << "  loops=" << loops << "\n";

        // Hashers
        hashfn::MS ms; ms.set_params(rng::get_u64(), rng::get_u64());
        hashfn::SimpleTab32 stab; stab.set_params();
        hashfn::TornadoTab32D1 tor1; tor1.set_params();
        hashfn::TornadoTab32D2 tor2; tor2.set_params();
        hashfn::TornadoTab32D3 tor3; tor3.set_params();
        hashfn::TornadoTab32D4 tor4; tor4.set_params();
        rapid::RapidHash32 rh32; rh32.set_params(rng::get_u64(), rapid_secret[0], rapid_secret[1], rapid_secret[2]);

        struct Row { const char* name; double mhps; double nsph; std::uint32_t checksum; };
        std::vector<Row> rows;
        auto push = [&](const char* name, double sec, std::uint32_t sink) {
            const std::size_t total = N * loops;
            rows.push_back({ name, (total / sec) / 1e6, (sec * 1e9) / double(total), sink });
        };

        auto do_u32 = [&](auto&& h, const char* name) {
            auto body = [&](volatile std::uint32_t& sink) {
                for (std::size_t i = 0; i < N; ++i) { const std::uint32_t x = load_le_u32(buf.data() + i * 4); sink ^= h.hash(x); }
            };
            auto [sec, s] = time_loops(loops, body); push(name, sec, s);
        };
        auto do_ptr4 = [&](auto&& h, const char* name) {
            auto body = [&](volatile std::uint32_t& sink) {
                for (std::size_t i = 0; i < N; ++i) { const void* p = buf.data() + i * 4; sink ^= h.hash(p, 4); }
            };
            auto [sec, s] = time_loops(loops, body); push(name, sec, s);
        };

        do_u32(ms,   "MS");
        do_u32(stab, "SimpleTab32");
        do_u32(tor1, "Tornado32_D1");
        do_u32(tor2, "Tornado32_D2");
        do_u32(tor3, "Tornado32_D3");
        do_u32(tor4, "Tornado32_D4");
        do_ptr4(rh32,"RapidHash32");

        std::ofstream f(out_csv, std::ios::binary);
        if (!f) { std::cerr << "Cannot open " << out_csv << "\n"; return 3; }
        f.setf(std::ios::fixed); f << std::setprecision(6);
        f << "function,Mhash_s,ns_per_hash,checksum_hex,loops,N\n";
        for (const auto& r : rows) {
            f << r.name << "," << r.mhps << "," << r.nsph << ",0x" << std::hex << r.checksum << std::dec << "," << loops << "," << N << "\n";
        }
        std::cout << "Wrote CSV: " << out_csv << "\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << "FATAL: " << e.what() << "\n"; return 1; }
}