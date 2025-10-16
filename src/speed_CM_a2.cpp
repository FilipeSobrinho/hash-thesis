
#include <cstdint>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>

#include "core/a2.hpp"
#include "core/randomgen.hpp"
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "sketch/bottomk.hpp"
#include "sketch/countmin.hpp"
#include "sketch/oph.hpp"
#include <random>
#include <algorithm>
#include <functional>
#include <unordered_map>

static inline std::uint32_t load_le_u32(const std::uint8_t* p) {
    return (std::uint32_t)p[0] | ((std::uint32_t)p[1] << 8) |
           ((std::uint32_t)p[2] << 16) | ((std::uint32_t)p[3] << 24);
}

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

// speed_CM_a2.cpp
// Measure time to INSERT all A2 keys into Count-Min Sketch, for multiple hash families.
struct RapidRow32 { rapid::RapidHash32 h; void set_seed(std::uint64_t s){ h.set_params(s, rapid_secret[0], rapid_secret[1], rapid_secret[2]); } std::uint32_t hash(std::uint32_t k) const { return h.hash(&k, sizeof(k)); } };

int main(int argc, char** argv) {
    try {
        std::size_t loops = 1000;
        std::size_t WIDTH = 32768;
        std::size_t DEPTH = 3;
        std::string out_csv = "a2_speed_cm.csv";
        int rounds = 10; // number of randomized passes

        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("missing value for " + a); };
            if (a == "--loops") loops = std::stoull(next());
            else if (a == "--width") WIDTH = std::stoull(next());
            else if (a == "--depth") DEPTH = std::stoull(next());
            else if (a == "--out") out_csv = next();
            else if (a == "--help" || a == "-h") {
                std::cout << "Usage: speed_CM_a2 [--loops L] [--width W] [--depth D] [--out file.csv]\n";
                return 0;
            }
        }

        datasets::A2 ds;
        const auto& buf = ds.buffer();
        const std::size_t N = ds.size();
        if (buf.size() != N * 4) { std::cerr << "A2 contiguous 4B expected\n"; return 2; }
        std::cout << "A2 items: " << N << "  loops=" << loops << "  W=" << WIDTH << " D=" << DEPTH << "\n";

        struct Row { const char* name; double mhps; double nsph; std::uint32_t checksum; };
        std::vector<Row> rows;
        auto push_row = [&](const char* name, double sec, std::uint32_t sink) {
            const std::size_t total = N * loops;
            rows.push_back({ name, (total / sec) / 1e6, (sec * 1e9) / double(total), sink });
        };

        // Seeds per depth (so we don't re-generate inside the timed body)
        std::vector<std::pair<std::uint64_t,std::uint64_t>> seedsMS(DEPTH);
        std::vector<std::uint64_t> seedsRapid(DEPTH);
        for (std::size_t d = 0; d < DEPTH; ++d) { seedsMS[d] = {rng::get_u64(), rng::get_u64()}; seedsRapid[d] = rng::get_u64(); }

        auto bench_ms = [&](){
            auto body = [&](volatile std::uint32_t& sink) {
                sketch::CountMin cms(WIDTH, DEPTH);
                std::vector<hashfn::MS> rows(DEPTH);
                for (std::size_t d=0; d<DEPTH; ++d) { rows[d].set_params(seedsMS[d].first, seedsMS[d].second); cms.set_row(d, &rows[d]); }
                for (std::size_t i = 0; i < N; ++i) { const std::uint32_t x = load_le_u32(buf.data() + i * 4); cms.add(x, 1); sink ^= x; }
            };
            return time_body(loops, body);
        };
        auto bench_stab = [&](){
            auto body = [&](volatile std::uint32_t& sink) {
                sketch::CountMin cms(WIDTH, DEPTH);
                std::vector<hashfn::SimpleTab32> rows(DEPTH);
                for (std::size_t d=0; d<DEPTH; ++d) { rows[d].set_params(); cms.set_row(d, &rows[d]); }
                for (std::size_t i = 0; i < N; ++i) { const std::uint32_t x = load_le_u32(buf.data() + i * 4); cms.add(x, 1); sink ^= x; }
            };
            return time_body(loops, body);
        };
        auto bench_tor = [&](int which){
            auto body = [&](volatile std::uint32_t& sink) {
                sketch::CountMin cms(WIDTH, DEPTH);
                if (which==1){ std::vector<hashfn::TornadoTab32D1> rows(DEPTH); for (std::size_t d=0; d<DEPTH; ++d){ rows[d].set_params(); cms.set_row(d, &rows[d]); }
                               for (std::size_t i=0;i<N;++i){ const std::uint32_t x=load_le_u32(buf.data()+i*4); cms.add(x,1); sink^=x; } }
                else if (which==2){ std::vector<hashfn::TornadoTab32D2> rows(DEPTH); for (std::size_t d=0; d<DEPTH; ++d){ rows[d].set_params(); cms.set_row(d, &rows[d]); }
                               for (std::size_t i=0;i<N;++i){ const std::uint32_t x=load_le_u32(buf.data()+i*4); cms.add(x,1); sink^=x; } }
                else if (which==3){ std::vector<hashfn::TornadoTab32D3> rows(DEPTH); for (std::size_t d=0; d<DEPTH; ++d){ rows[d].set_params(); cms.set_row(d, &rows[d]); }
                               for (std::size_t i=0;i<N;++i){ const std::uint32_t x=load_le_u32(buf.data()+i*4); cms.add(x,1); sink^=x; } }
                else { std::vector<hashfn::TornadoTab32D4> rows(DEPTH); for (std::size_t d=0; d<DEPTH; ++d){ rows[d].set_params(); cms.set_row(d, &rows[d]); }
                               for (std::size_t i=0;i<N;++i){ const std::uint32_t x=load_le_u32(buf.data()+i*4); cms.add(x,1); sink^=x; } }
            };
            return time_body(loops, body);
        };
        auto bench_rapid = [&](){
            auto body = [&](volatile std::uint32_t& sink) {
                sketch::CountMin cms(WIDTH, DEPTH);
                std::vector<RapidRow32> rows(DEPTH);
                for (std::size_t d=0; d<DEPTH; ++d) { rows[d].set_seed(seedsRapid[d]); cms.set_row(d, &rows[d]); }
                for (std::size_t i=0; i<N; ++i) { const std::uint32_t x=load_le_u32(buf.data()+i*4); cms.add(x,1); sink ^= x; }
            };
            return time_body(loops, body);
        };

        auto [sec_ms, s_ms]     = bench_ms();     rows.push_back({"MS",             (N*loops/sec_ms)/1e6, (sec_ms*1e9)/(N*loops), s_ms});
        auto [sec_st, s_st]     = bench_stab();   rows.push_back({"SimpleTab32",    (N*loops/sec_st)/1e6, (sec_st*1e9)/(N*loops), s_st});
        auto [sec_t1, s_t1]     = bench_tor(1);   rows.push_back({"Tornado32_D1",   (N*loops/sec_t1)/1e6, (sec_t1*1e9)/(N*loops), s_t1});
        auto [sec_t2, s_t2]     = bench_tor(2);   rows.push_back({"Tornado32_D2",   (N*loops/sec_t2)/1e6, (sec_t2*1e9)/(N*loops), s_t2});
        auto [sec_t3, s_t3]     = bench_tor(3);   rows.push_back({"Tornado32_D3",   (N*loops/sec_t3)/1e6, (sec_t3*1e9)/(N*loops), s_t3});
        auto [sec_t4, s_t4]     = bench_tor(4);   rows.push_back({"Tornado32_D4",   (N*loops/sec_t4)/1e6, (sec_t4*1e9)/(N*loops), s_t4});
        auto [sec_rh, s_rh]     = bench_rapid();  rows.push_back({"RapidHash32",    (N*loops/sec_rh)/1e6, (sec_rh*1e9)/(N*loops), s_rh});
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
        f << "function,Mhash_s,ns_per_hash,checksum_hex,loops,N,width,depth\n";
        for (const auto& r : rows) {
            f << r.name << "," << r.mhps << "," << r.nsph << ",0x" << std::hex << r.checksum << std::dec
              << "," << loops << "," << N << "," << WIDTH << "," << DEPTH << "\n";
        }
        std::cout << "Wrote: " << out_csv << "\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << "FATAL: " << e.what() << "\n"; return 1; }
}
