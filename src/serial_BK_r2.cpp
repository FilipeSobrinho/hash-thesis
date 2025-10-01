// serial_BK_r2.cpp
// Bottom-k accuracy, single-threaded, R2 dataset (variable-length words).
// CSV: function,rep,relerr
// Tornado D1..D4 variants included.

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "sketch/bottomk.hpp"
#include "core/randomgen.hpp"
#include "hash/msvec.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "core/r2.hpp"

struct View {
    const std::uint8_t* p;
    std::uint32_t len;
    bool operator==(const View& o) const noexcept {
        return len == o.len && std::memcmp(p, o.p, len) == 0;
    }
};
struct ViewHash {
    std::size_t operator()(const View& v) const noexcept {
        std::uint32_t h = 2166136261u;
        const std::uint8_t* s = v.p;
        for (std::uint32_t i = 0; i < v.len; ++i) { h ^= s[i]; h *= 16777619u; }
        return (std::size_t)h;
    }
};

int main(int argc, char** argv) {
    std::size_t K = 24'500;
    std::size_t R = 50'000;
    std::string outfile = "bottomk_r2_relerr.csv";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 < argc) return std::string(argv[++i]);
            throw std::runtime_error("Missing value for " + arg);
        };
        if (arg == "--k") K = std::stoull(next());
        else if (arg == "--R") R = std::stoull(next());
        else if (arg == "--out") outfile = next();
        else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: serial_BK_r2 [--k 24500] [--R 1000] [--out file.csv]\n";
            return 0;
        }
    }

    std::cout << "Bottom-k accuracy (R2) single-threaded\n"
              << "  k=" << K << "  R=" << R << "\n"
              << "Writing: " << outfile << "\n";

    std::ofstream out(outfile, std::ios::binary);
    if (!out) { std::cerr << "Cannot open output file: " << outfile << "\n"; return 1; }
    out.setf(std::ios::fixed); out << std::setprecision(8);
    out << "function,rep,relerr\n";

    datasets::R2 base;
    const auto& buf = base.buffer();
    const auto& index = base.index();
    const std::size_t ITEMS = index.size();
    if (ITEMS == 0) { std::cerr << "R2: no items\n"; return 2; }

    std::unordered_set<View, ViewHash> uniq;
    uniq.reserve(ITEMS);
    for (std::size_t i = 0; i < ITEMS; ++i) {
        const auto [off, len] = index[i];
        uniq.insert(View{ buf.data() + off, len });
    }
    const double Dtrue = static_cast<double>(uniq.size());

    enum {
        IDX_MSVEC,
        IDX_TAB_ON_MSVEC,
        IDX_TOR_ON_MSVEC_D1,
        IDX_TOR_ON_MSVEC_D2,
        IDX_TOR_ON_MSVEC_D3,
        IDX_TOR_ON_MSVEC_D4,
        IDX_RAPID32,
        NUM_FUNCS
    };
    const char* NAMES[NUM_FUNCS] = {
        "MSVec","TabOnMSVec","TornadoOnMSVecD1","TornadoOnMSVecD2","TornadoOnMSVecD3","TornadoOnMSVecD4","RapidHash32"
    };

    using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;
    struct RepParams { Coeffs coeffs; std::uint64_t rapid_seed; };
    std::vector<RepParams> params(R);
    for (std::size_t r = 0; r < R; ++r) {
        for (std::size_t i = 0; i < MSVEC_NUM_COEFFS; ++i) params[r].coeffs[i] = rng::get_u64();
        params[r].rapid_seed = rng::get_u64();
    }

    auto run_one = [&](int idx,
                       const hashfn::MSVec& msvec,
                       const hashfn::TabOnMSVec& tabms,
                       const hashfn::TornadoOnMSVecD1& tor1,
                       const hashfn::TornadoOnMSVecD2& tor2,
                       const hashfn::TornadoOnMSVecD3& tor3,
                       const hashfn::TornadoOnMSVecD4& tor4,
                       const rapid::RapidHash32& rh32) -> double
    {
        sketch::BottomK bk(K);
        switch (idx) {
        case IDX_MSVEC:
            for (std::size_t i = 0; i < ITEMS; ++i) { const auto [off,len]=index[i]; const void* p=buf.data()+off; bk.push(msvec.hash(p,len)); } break;
        case IDX_TAB_ON_MSVEC:
            for (std::size_t i = 0; i < ITEMS; ++i) { const auto [off,len]=index[i]; const void* p=buf.data()+off; bk.push(tabms.hash(p,len)); } break;
        case IDX_TOR_ON_MSVEC_D1:
            for (std::size_t i = 0; i < ITEMS; ++i) { const auto [off,len]=index[i]; const void* p=buf.data()+off; bk.push(tor1.hash(p,len)); } break;
        case IDX_TOR_ON_MSVEC_D2:
            for (std::size_t i = 0; i < ITEMS; ++i) { const auto [off,len]=index[i]; const void* p=buf.data()+off; bk.push(tor2.hash(p,len)); } break;
        case IDX_TOR_ON_MSVEC_D3:
            for (std::size_t i = 0; i < ITEMS; ++i) { const auto [off,len]=index[i]; const void* p=buf.data()+off; bk.push(tor3.hash(p,len)); } break;
        case IDX_TOR_ON_MSVEC_D4:
            for (std::size_t i = 0; i < ITEMS; ++i) { const auto [off,len]=index[i]; const void* p=buf.data()+off; bk.push(tor4.hash(p,len)); } break;
        case IDX_RAPID32:
            for (std::size_t i = 0; i < ITEMS; ++i) { const auto [off,len]=index[i]; const void* p=buf.data()+off; bk.push(rh32.hash(p,len)); } break;
        }
        const double est = bk.estimate();
        return (est - Dtrue) / Dtrue;
    };

    for (std::size_t r = 0; r < R; ++r) {
        hashfn::MSVec msvec;           msvec.set_params(params[r].coeffs, true);
        hashfn::TabOnMSVec tabms;      tabms.set_params(params[r].coeffs, true);
        hashfn::TornadoOnMSVecD1 tor1; tor1.set_params(params[r].coeffs, true);
        hashfn::TornadoOnMSVecD2 tor2; tor2.set_params(params[r].coeffs, true);
        hashfn::TornadoOnMSVecD3 tor3; tor3.set_params(params[r].coeffs, true);
        hashfn::TornadoOnMSVecD4 tor4; tor4.set_params(params[r].coeffs, true);

        rapid::RapidHash32 rh32;
        rh32.set_params(params[r].rapid_seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

        for (int f = 0; f < NUM_FUNCS; ++f) {
            const double relerr = run_one(f, msvec, tabms, tor1, tor2, tor3, tor4, rh32);
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
