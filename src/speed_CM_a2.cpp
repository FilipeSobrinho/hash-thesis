// speed_CM_a2.cpp
// Measure time to INSERT all A2 4-byte keys into Count-Min Sketch, for multiple hash families.
// Structure aligned with speed_CM_r1.cpp and speed_OPH_a2.cpp.

#include <cstdint>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>
#include <array>
#include <random>
#include <algorithm>
#include <functional>
#include <unordered_map>

#include "core/a2.hpp"
#include "core/randomgen.hpp"

#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"

#include "sketch/countmin.hpp"

// ----------------- utils -----------------
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

// Row hash for CMS (maps 32-bit key -> column)
struct RowHash32 {
    std::uint32_t a = 0, b = 0;
    void set_seed(std::uint64_t s) { a = (std::uint32_t)s | 1u; b = (std::uint32_t)(s >> 32); }
    std::uint32_t hash(std::uint32_t x) const { return a * x + b; } // mod 2^32
};

int main(int argc, char** argv) {
    try {
        // ----------------- CLI -----------------
        std::size_t loops = 1000;
        std::size_t WIDTH = 32768;
        std::size_t DEPTH = 3;
        std::string out_csv = "a2_speed_cm.csv";
        int rounds = 10; // randomized passes

        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() {
                if (i + 1 < argc) return std::string(argv[++i]);
                throw std::runtime_error("missing value for " + a);
                };
            if (a == "--loops") loops = std::stoull(next());
            else if (a == "--width") WIDTH = std::stoull(next());
            else if (a == "--depth") DEPTH = std::stoull(next());
            else if (a == "--out") out_csv = next();
            else if (a == "--help" || a == "-h") {
                std::cout << "Usage: speed_CM_a2 [--loops L] [--width W] [--depth D] [--out file.csv]\n";
                return 0;
            }
        }

        // ----------------- dataset -----------------
        datasets::A2 ds;
        const auto& buf = ds.buffer();
        const std::size_t N = ds.size();
        if (buf.size() != N * 4) {
            std::cerr << "A2 contiguous 4B expected\n";
            return 2;
        }
        std::cout << "A2 items: " << N << "  loops=" << loops << "  W=" << WIDTH << " D=" << DEPTH << "\n";

        // ----------------- hash families under test (key -> 32-bit hv) -----------------
        hashfn::MS ms;                     ms.set_params(rng::get_u64(), rng::get_u64());
        hashfn::SimpleTab32 stab;          stab.set_params();
        hashfn::TornadoTab32D1 t1;         t1.set_params();
        hashfn::TornadoTab32D2 t2;         t2.set_params();
        hashfn::TornadoTab32D3 t3;         t3.set_params();
        hashfn::TornadoTab32D4 t4;         t4.set_params();
        rapid::RapidHash32 rh;             rh.set_params(rng::get_u64(), rapid_secret[0], rapid_secret[1], rapid_secret[2]);

        // ----------------- fixed row hashers for CMS (seeded once) -----------------
        std::vector<RowHash32> rows(DEPTH);
        for (std::size_t d = 0; d < DEPTH; ++d) rows[d].set_seed(rng::get_u64());

        struct Row { const char* name; double mhps; double nsph; std::uint32_t checksum; };
        std::vector<Row> out;
        auto push_row = [&](const char* name, double sec, std::uint32_t sink) {
            const std::size_t total = N * loops;
            out.push_back({ name, (total / sec) / 1e6, (sec * 1e9) / double(total), sink });
            };

        // ----------------- benches -----------------
        auto bench_u32 = [&](auto& H, const char* name) {
            auto body = [&](volatile std::uint32_t& sink) {
                sketch::CountMin cms(WIDTH, DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) cms.set_row(d, &rows[d]);
                for (std::size_t i = 0; i < N; ++i) {
                    const std::uint32_t x = load_le_u32(buf.data() + i * 4);
                    const std::uint32_t hv = H.hash(x);
                    cms.add(hv, 1);
                    sink ^= hv;
                }
                };
            auto [sec, s] = time_body(loops, body);
            push_row(name, sec, s);
            };
        auto bench_ptr = [&](auto& H, const char* name) {
            auto body = [&](volatile std::uint32_t& sink) {
                sketch::CountMin cms(WIDTH, DEPTH);
                for (std::size_t d = 0; d < DEPTH; ++d) cms.set_row(d, &rows[d]);
                for (std::size_t i = 0; i < N; ++i) {
                    const void* p = buf.data() + i * 4;
                    const std::uint32_t hv = H.hash(p, 4);
                    cms.add(hv, 1);
                    sink ^= hv;
                }
                };
            auto [sec, s] = time_body(loops, body);
            push_row(name, sec, s);
            };

        // ----------------- randomized job ordering over rounds -----------------
        struct Job { const char* name; std::function<void()> run; };
        std::vector<Job> jobs = {
            {"MS",            [&] { bench_u32(ms,   "MS"); }},
            {"SimpleTab32",   [&] { bench_u32(stab, "SimpleTab32"); }},
            {"Tornado32_D1",  [&] { bench_u32(t1,   "Tornado32_D1"); }},
            {"Tornado32_D2",  [&] { bench_u32(t2,   "Tornado32_D2"); }},
            {"Tornado32_D3",  [&] { bench_u32(t3,   "Tornado32_D3"); }},
            {"Tornado32_D4",  [&] { bench_u32(t4,   "Tornado32_D4"); }},
            {"RapidHash32",   [&] { bench_ptr(rh,   "RapidHash32"); }},
        };

        for (int r = 0; r < rounds; ++r) {
            std::mt19937_64 ord_rng(std::random_device{}());
            std::shuffle(jobs.begin(), jobs.end(), ord_rng);
            for (auto& j : jobs) j.run();
        }

        // ----------------- collapse by median (per function) -----------------
        {
            std::unordered_map<std::string, std::vector<Row>> groups;
            groups.reserve(out.size());
            for (const auto& r : out) groups[std::string(r.name)].push_back(r);

            std::vector<Row> collapsed; collapsed.reserve(groups.size());
            auto median = [](std::vector<double>& v) {
                if (v.empty()) return 0.0;
                size_t n = v.size();
                std::nth_element(v.begin(), v.begin() + n / 2, v.end());
                double m = v[n / 2];
                if (n % 2 == 0) {
                    auto max_lower = *std::max_element(v.begin(), v.begin() + n / 2);
                    m = 0.5 * (m + max_lower);
                }
                return m;
                };

            for (auto& kv : groups) {
                const char* nm = kv.second.front().name;
                std::vector<double> mhpsv; mhpsv.reserve(kv.second.size());
                std::vector<double> nsphv; nsphv.reserve(kv.second.size());
                std::uint32_t chk = 0;
                for (auto& r : kv.second) {
                    mhpsv.push_back(r.mhps);
                    nsphv.push_back(r.nsph);
                    chk ^= r.checksum;
                }
                double mhps_med = median(mhpsv);
                double nsph_med = median(nsphv);
                collapsed.push_back(Row{ nm, mhps_med, nsph_med, chk });
            }
            out.swap(collapsed);
        }

        // ----------------- write CSV -----------------
        std::ofstream f(out_csv, std::ios::binary);
        if (!f) { std::cerr << "Cannot open " << out_csv << "\n"; return 3; }
        f.setf(std::ios::fixed); f << std::setprecision(6);
        f << "function,Mhash_s,ns_per_hash,checksum_hex,loops,N,width,depth\n";
        for (const auto& r : out) {
            f << r.name << "," << r.mhps << "," << r.nsph << ",0x" << std::hex << r.checksum << std::dec
                << "," << loops << "," << N << "," << WIDTH << "," << DEPTH << "\n";
        }
        std::cout << "Wrote: " << out_csv << "\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
}
