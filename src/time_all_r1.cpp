// time_all_r1.cpp
// Hashing speed on R1 (20-byte keys) for:
// MSVec, TabOnMSVec, TornadoOnMSVec D1..D4, RapidHash32.
// Always writes CSV (default: r1_speed.csv).
// CLI: --loops L  --out file.csv  --help
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <array>
#include "core/r1.hpp"
#include "core/randomgen.hpp"
#include "hash/msvec.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"

template <typename Body>
static std::pair<double, std::uint32_t> time_loops(std::size_t loops, Body&& body) {
    volatile std::uint32_t sink = 0; body(sink);
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t L = 0; L < loops; ++L) body(sink);
    const auto t1 = std::chrono::steady_clock::now();
    return { std::chrono::duration<double>(t1 - t0).count(), sink };
}

int main(int argc, char** argv) {
    try {
        std::size_t loops = 1000;
        std::string out_csv = "r1_speed.csv";
        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("missing value for " + a); };
            if (a == "--loops") loops = std::stoull(next());
            else if (a == "--out") out_csv = next();
            else if (a == "--help" || a == "-h") { std::cout << "Usage: time_all_r1 [--loops L] [--out file.csv]\n"; return 0; }
        }

        datasets::R1 ds;
        const auto& raw = ds.buffer();
        const std::size_t N = ds.size(); // N items of 20 bytes

        using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;
        Coeffs coeffs; for (auto& c : coeffs) c = rng::get_u64();
        hashfn::MSVec msvec;           msvec.set_params(coeffs, true);
        hashfn::TabOnMSVec tabms;      tabms.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD1 t1;   t1.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD2 t2;   t2.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD3 t3;   t3.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD4 t4;   t4.set_params(coeffs, true);
        rapid::RapidHash32 rh32;       rh32.set_params(rng::get_u64(), rapid_secret[0], rapid_secret[1], rapid_secret[2]);

        struct Row { const char* name; double mhps; double nsph; std::uint32_t checksum; };
        std::vector<Row> rows;
        auto push = [&](const char* name, double sec, std::uint32_t sink) {
            const std::size_t total = N * loops;
            rows.push_back({ name, (total / sec) / 1e6, (sec * 1e9) / double(total), sink });
        };

        auto do_family = [&](auto&& h, const char* name) {
            auto body = [&](volatile std::uint32_t& sink) {
                for (std::size_t i = 0; i < N; ++i) { const void* p = raw.data() + i * 20; sink ^= h.hash(p, 20); }
            };
            auto [sec, s] = time_loops(loops, body); push(name, sec, s);
        };

        do_family(msvec, "MSVec");
        do_family(tabms, "TabOnMSVec");
        do_family(t1,    "TornadoOnMSVecD1");
        do_family(t2,    "TornadoOnMSVecD2");
        do_family(t3,    "TornadoOnMSVecD3");
        do_family(t4,    "TornadoOnMSVecD4");
        do_family(rh32,  "RapidHash32");

        std::ofstream f(out_csv, std::ios::binary);
        if (!f) { std::cerr << "Cannot open " << out_csv << "\n"; return 3; }
        f.setf(std::ios::fixed); f << std::setprecision(6);
        f << "function,Mhash_s,ns_per_hash,checksum_hex,loops,N\n";
        for (const auto& r : rows) { f << r.name << "," << r.mhps << "," << r.nsph << ",0x" << std::hex << r.checksum << std::dec << "," << loops << "," << N << "\n"; }
        std::cout << "Wrote CSV: " << out_csv << "\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << "FATAL: " << e.what() << "\n"; return 1; }
}