
#include <cstdint>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>
#include <array>

#include "core/randomgen.hpp"
#include "hash/rapidhash.h"
#include "hash/tornado32.hpp"
#include "hash/msvec.hpp"
#include "sketch/bottomk.hpp"
#include "sketch/countmin.hpp"
#include "sketch/oph.hpp"
#include "hash/simpletab32.hpp"


#if defined(_MSC_VER)
#define FINLINE __forceinline
#else
#define FINLINE inline __attribute__((always_inline))
#endif

template <class Body>
static std::pair<double, std::uint32_t> time_body(std::size_t loops, Body&& body) {
    volatile std::uint32_t sink = 0;
    body(sink); // warmup
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t L = 0; L < loops; ++L) body(sink);
    const auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    return { sec, sink };
}

// speed_BK_r2.cpp
#include "core/r2.hpp"
#include <random>
#include <algorithm>
#include <functional>
#include <unordered_map>

int main(int argc, char** argv) {
    try {
        std::size_t loops = 1000;
        std::size_t K = 24500;
        std::string out_csv = "r2_speed_bk.csv";
        int rounds = 10; // number of randomized passes

        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("missing value for " + a); };
            if (a == "--loops") loops = std::stoull(next());
            else if (a == "--K") K = std::stoull(next());
            else if (a == "--out") out_csv = next();
            else if (a == "--help" || a == "-h") { std::cout << "Usage: speed_BK_r2 [--loops L] [--K K] [--out file.csv]\n"; return 0; }
        }

        datasets::R2 ds;
        const auto& buf = ds.buffer();
        const auto& index = ds.index();
        const std::size_t N = index.size();
        std::cout << "R2 items: " << N << " loops=" << loops << " K=" << K << "\n";

        using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;
        Coeffs coeffs; for (auto& c : coeffs) c = rng::get_u64();
        hashfn::MSVec msvec;           msvec.set_params(coeffs, true);
        hashfn::TabOnMSVec tabms;      tabms.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD1 t1;   t1.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD2 t2;   t2.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD3 t3;   t3.set_params(coeffs, true);
        hashfn::TornadoOnMSVecD4 t4;   t4.set_params(coeffs, true);
        rapid::RapidHash32 rh;         rh.set_params(rng::get_u64(), rapid_secret[0], rapid_secret[1], rapid_secret[2]);

        struct Row { const char* name; double mhps; double nsph; std::uint32_t checksum; };
        std::vector<Row> rows;
        auto push_row = [&](const char* name, double sec, std::uint32_t sink) {
            const std::size_t total = N * loops;
            rows.push_back({ name, (total / sec) / 1e6, (sec * 1e9) / double(total), sink });
        };

        auto bench_fam = [&](auto& H, const char* name) {
            auto body = [&](volatile std::uint32_t& sink) {
                sketch::BottomK bk((std::uint32_t)K);
                for (std::size_t i = 0; i < N; ++i) {
                    const auto off = index[i].first;
                    const auto len = index[i].second;
                    bk.push(H.hash(buf.data() + off, len));
                    sink ^= (unsigned)i;
                }
            };
            auto [sec, s] = time_body(loops, body);
            push_row(name, sec, s);
        };

struct Job { const char* name; std::function<void()> run; };
std::vector<Job> jobs = {
	{"MSVec", [&] { bench_fam(msvec, "MSVec"); }},
    {"TabOnMSVec", [&]{ bench_fam(tabms, "TabOnMSVec"); }},
    {"TornadoOnMSVecD1", [&]{ bench_fam(t1, "TornadoOnMSVecD1"); }},
    {"TornadoOnMSVecD2", [&]{ bench_fam(t2, "TornadoOnMSVecD2"); }},
    {"TornadoOnMSVecD3", [&]{ bench_fam(t3, "TornadoOnMSVecD3"); }},
    {"TornadoOnMSVecD4", [&]{ bench_fam(t4, "TornadoOnMSVecD4"); }},
    {"RapidHash32", [&]{ bench_fam(rh, "RapidHash32"); }},
};
for (int _r = 0; _r < rounds; ++_r) {
    std::mt19937_64 _ord_rng(std::random_device{}());
    std::shuffle(jobs.begin(), jobs.end(), _ord_rng);
    for (auto& j : jobs) j.run();
}
{
    std::unordered_map<std::string, std::vector<Row>> _groups;
    _groups.reserve(rows.size());
    for (const auto& r : rows) _groups[std::string(r.name)].push_back(r);
    std::vector<Row> _collapsed; _collapsed.reserve(_groups.size());
    auto _median = [](std::vector<double>& v) {
        if (v.empty()) return 0.0;
        size_t n = v.size();
        std::nth_element(v.begin(), v.begin() + n/2, v.end());
        double m = v[n/2];
        if (n % 2 == 0) {
            auto max_lower = *std::max_element(v.begin(), v.begin() + n/2);
            m = 0.5 * (m + max_lower);
        }
        return m;
    };
    for (auto& kv : _groups) {
        const char* nm = kv.second.front().name;
        std::vector<double> mhpsv; mhpsv.reserve(kv.second.size());
        std::vector<double> nsphv; nsphv.reserve(kv.second.size());
        std::uint32_t chk = 0;
        for (auto& r : kv.second) { mhpsv.push_back(r.mhps); nsphv.push_back(r.nsph); chk ^= r.checksum; }
        double mhps_med = _median(mhpsv);
        double nsph_med = _median(nsphv);
        _collapsed.push_back(Row{ nm, mhps_med, nsph_med, chk });
    }
    rows.swap(_collapsed);
}


        std::ofstream f(out_csv, std::ios::binary);
        if (!f) { std::cerr << "Cannot open " << out_csv << "\n"; return 3; }
        f.setf(std::ios::fixed); f << std::setprecision(6);
        f << "function,Mhash_s,ns_per_hash,checksum_hex,loops,N,K\n";
        for (const auto& r : rows) {
            f << r.name << "," << r.mhps << "," << r.nsph << ",0x" << std::hex << r.checksum << std::dec
              << "," << loops << "," << N << "," << K << "\n";
        }
        std::cout << "Wrote: " << out_csv << "\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << "FATAL: " << e.what() << "\n"; return 1; }
}
