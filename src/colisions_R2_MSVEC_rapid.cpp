// collisions_R2_repeat.cpp
// Repeat a collision study over R2 UNIQUE keys only, many times,
// and report the MAX number of collisions seen across repetitions.
//
// - Supports both R2 index layouts:
//     (1) std::vector<size_t> with sentinel (size N+1)  -> offsets
//     (2) std::vector<std::pair<uint32_t,uint32_t>> (size N) -> (start,len)
// - Each repetition re-randomizes MSVec and RapidHash32 parameters.
// - Uses an efficient 32-bit radix sort per trial to count collisions.
//
// Build: same as other drivers (Release | x64)
// CLI:   --trials T   (default: 50000)
//        --out FILE   (default: r2_collision_max.csv)
//        --quiet      (less console output)
//        --help/-h

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>
#include <array>
#include <algorithm>
#include <type_traits>
#include <chrono>

#include "core/r2.hpp"
#include "core/randomgen.hpp"
#include "hash/msvec.hpp"
#include "hash/rapidhash.h"

// ---------- R2 accessors that handle both index layouts ----------

template<class Index>
static inline std::pair<const void*, std::size_t>
get_item_r2(const std::vector<std::uint8_t>& buf, const Index& index, std::size_t i) {
    using Elem = typename Index::value_type;
    if constexpr (std::is_same_v<Elem, std::size_t>) {
        // Offsets-with-sentinel: index[i]..index[i+1]
        const std::size_t start = index[i];
        const std::size_t end = index[i + 1];
        return { buf.data() + start, end - start };
    }
    else if constexpr (std::is_same_v<Elem, std::pair<std::uint32_t, std::uint32_t>>) {
        // Start/len pairs
        const std::uint32_t start = index[i].first;
        const std::uint32_t len = index[i].second;
        return { buf.data() + start, len };
    }
    else {
        static_assert(!sizeof(Index*), "Unsupported R2 index type");
        return { nullptr, 0 };
    }
}

template<class Index>
static inline std::size_t r2_count_items(const Index& index) {
    using Elem = typename Index::value_type;
    if constexpr (std::is_same_v<Elem, std::size_t>) {
        return (index.size() >= 1 ? index.size() - 1 : 0);
    }
    else if constexpr (std::is_same_v<Elem, std::pair<std::uint32_t, std::uint32_t>>) {
        return index.size();
    }
    else {
        static_assert(!sizeof(Index*), "Unsupported R2 index type");
        return 0;
    }
}

// ---------- 64-bit content fingerprint (to detect repeated keys) ----------
// FNV-1a 64: fast and sufficient to dedupe 3e4–1e6 keys.

static inline std::uint64_t fnv1a64(const void* data, std::size_t len) {
    const std::uint8_t* p = static_cast<const std::uint8_t*>(data);
    std::uint64_t h = 1469598103934665603ull; // offset basis
    for (std::size_t i = 0; i < len; ++i) {
        h ^= std::uint64_t(p[i]);
        h *= 1099511628211ull; // prime
    }
    return h;
}

// ---------- Small slice descriptor for unique keys ----------

struct Slice { std::uint32_t start; std::uint32_t len; };

// Extract UNIQUE keys only (by 64-bit content fingerprint), and return their slices.
template <class Index>
static std::vector<Slice> extract_unique_slices(const std::vector<std::uint8_t>& buf,
    const Index& index,
    std::size_t& N_items,
    std::size_t& N_unique)
{
    const std::size_t N = r2_count_items(index);
    N_items = N;

    // First pass: dedupe by 64-bit content fingerprint
    std::unordered_map<std::uint64_t, Slice> uniq;
    uniq.reserve(N * 2);

    for (std::size_t i = 0; i < N; ++i) {
        auto [p, len] = get_item_r2(buf, index, i);
        // keep zero-length? We exclude (they would all be identical)
        if (len == 0) continue;
        const std::uint64_t fp = fnv1a64(p, len);
        if (uniq.find(fp) == uniq.end()) {
            const auto start = (std::uint32_t)(static_cast<const std::uint8_t*>(p) - buf.data());
            uniq.emplace(fp, Slice{ start, (std::uint32_t)len });
        }
    }

    // Materialize unique slices
    std::vector<Slice> out;
    out.reserve(uniq.size());
    for (auto& kv : uniq) out.push_back(kv.second);

    N_unique = out.size();
    return out;
}

// ---------- Fast 32-bit LSD Radix Sort (4 passes) ----------

static inline void radix_sort_u32(std::vector<std::uint32_t>& a) {
    constexpr int B = 256;
    constexpr int PASSES = 4;
    std::vector<std::uint32_t> aux(a.size());
    for (int pass = 0; pass < PASSES; ++pass) {
        std::uint32_t shift = pass * 8;
        std::size_t cnt[B] = {};
        for (std::uint32_t v : a) cnt[(v >> shift) & 0xFFu]++;
        std::size_t sum = 0;
        for (int i = 0; i < B; ++i) { std::size_t c = cnt[i]; cnt[i] = sum; sum += c; }
        for (std::uint32_t v : a) aux[cnt[(v >> shift) & 0xFFu]++] = v;
        a.swap(aux);
    }
}

// Count collisions (N - distinct) from a sorted vector of 32-bit hashes.
static inline std::uint32_t count_collisions_sorted(const std::vector<std::uint32_t>& h) {
    if (h.empty()) return 0;
    std::uint32_t distinct = 1;
    for (std::size_t i = 1; i < h.size(); ++i) if (h[i] != h[i - 1]) ++distinct;
    return (std::uint32_t)(h.size() - distinct);
}

// ---------- One trial for a given hasher over unique slices ----------

template <class Hasher>
static std::uint32_t trial_collisions(Hasher& H,
    const std::vector<std::uint8_t>& buf,
    const std::vector<Slice>& uniq)
{
    std::vector<std::uint32_t> hv;
    hv.resize(uniq.size());
    for (std::size_t i = 0; i < uniq.size(); ++i) {
        const void* p = buf.data() + uniq[i].start;
        hv[i] = H.hash(p, uniq[i].len);  // full 32-bit value
    }
    radix_sort_u32(hv);
    return count_collisions_sorted(hv);
}

int main(int argc, char** argv) {
    try {
        std::uint32_t trials = 50000;
        std::string out_csv = "r2_collision_max.csv";
        bool quiet = false;

        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() {
                if (i + 1 < argc) return std::string(argv[++i]);
                throw std::runtime_error("missing value for " + a);
                };
            if (a == "--trials") trials = (std::uint32_t)std::stoul(next());
            else if (a == "--out") out_csv = next();
            else if (a == "--quiet") quiet = true;
            else if (a == "--help" || a == "-h") {
                std::cout
                    << "Usage: collisions_R2_repeat [--trials T] [--out file.csv] [--quiet]\n"
                    << "Runs T repetitions over R2 UNIQUE keys, with fresh random params each time,\n"
                    << "and reports MAX collisions observed for MSVec and RapidHash32.\n";
                return 0;
            }
            else {
                throw std::runtime_error("unknown option: " + a);
            }
        }

        // Load dataset R2
        datasets::R2 ds;
        const auto& buf = ds.buffer();
        const auto& index = ds.index();

        std::size_t N_items = 0, N_unique = 0;
        std::vector<Slice> uniq = extract_unique_slices(buf, index, N_items, N_unique);
        if (!quiet) {
            std::cout << "R2 items: " << N_items
                << " | unique keys: " << N_unique
                << " | repeats: " << (N_items - N_unique) << "\n";
            std::cout << "Trials: " << trials << "\n";
        }

        // Prepare hasher instances (params will be set each trial)
        hashfn::MSVec msvec;
        rapid::RapidHash32 rh;

        std::uint32_t max_coll_ms = 0, max_coll_rh = 0;
        std::uint32_t argmax_ms = 0, argmax_rh = 0;

        auto t0 = std::chrono::high_resolution_clock::now();

        for (std::uint32_t t = 1; t <= trials; ++t) {
            // Fresh random params each trial
            {
                using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;
                Coeffs coeffs; for (auto& c : coeffs) c = rng::get_u64();
                msvec.set_params(coeffs, true);
            }
            {
                std::uint64_t seed = rng::get_u64();
                rh.set_params(seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);
            }

            std::uint32_t c_ms = trial_collisions(msvec, buf, uniq);
            std::uint32_t c_rh = trial_collisions(rh, buf, uniq);

            if (c_ms > max_coll_ms) { max_coll_ms = c_ms; argmax_ms = t; }
            if (c_rh > max_coll_rh) { max_coll_rh = c_rh; argmax_rh = t; }

            if (!quiet && (t % 1000 == 0 || t == trials)) {
                std::cout << "Trial " << t << "/" << trials
                    << " | max_MSVec=" << max_coll_ms
                    << " (at " << argmax_ms << ")"
                    << " | max_RapidHash32=" << max_coll_rh
                    << " (at " << argmax_rh << ")\n";
            }
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        if (!quiet) {
            double secs = std::chrono::duration<double>(t1 - t0).count();
            std::cout << "Done in " << std::fixed << std::setprecision(2) << secs << " s\n";
        }

        // Console summary
        std::cout << "MAX collisions over " << trials << " trials (UNIQUE keys only):\n";
        std::cout << "  MSVec       : " << max_coll_ms << " (trial " << argmax_ms << ")\n";
        std::cout << "  RapidHash32 : " << max_coll_rh << " (trial " << argmax_rh << ")\n";

        // CSV
        std::ofstream f(out_csv, std::ios::binary);
        if (!f) { std::cerr << "Cannot open " << out_csv << "\n"; return 3; }
        f << "dataset,unique_keys,trials,max_collisions_msvec,trial_msvec,max_collisions_rapidhash32,trial_rapidhash32\n";
        f << "R2," << N_unique << "," << trials << ","
            << max_coll_ms << "," << argmax_ms << ","
            << max_coll_rh << "," << argmax_rh << "\n";
        f.close();

        std::cout << "Wrote: " << out_csv << "\n";
        return 0;

    }
    catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
}
