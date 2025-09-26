#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <iostream>

#if __has_include(<filesystem>)
#include <filesystem>
#endif

#include "core/a2.hpp"   // uses datasets::A2 and datasets::A2Split
#include "core/a1.hpp"   // for datasets::StreamPtr API compatibility

static inline std::uint32_t load_le_u32(const void* p) {
    const std::uint8_t* b = static_cast<const std::uint8_t*>(p);
    return (std::uint32_t)b[0]
        | (std::uint32_t(b[1]) << 8)
        | (std::uint32_t(b[2]) << 16)
        | (std::uint32_t(b[3]) << 24);
}

// 32-bit FNV-1a over the 4B items (quick checksum)
static inline std::uint32_t fnv1a32(const std::uint8_t* data, std::size_t nbytes) {
    const std::uint32_t FNV_PRIME = 16777619u;
    std::uint32_t h = 2166136261u;
    for (std::size_t i = 0; i < nbytes; ++i) {
        h ^= data[i];
        h *= FNV_PRIME;
    }
    return h;
}

int main() {
    try {
        // Helpful runtime context
#if __has_include(<filesystem>)
        std::cerr << "cwd: " << std::filesystem::current_path().string() << "\n";
#endif

        // NOTE: a2.hpp you attached hardcodes datasets::filepath internally
        // to something like C:/Users/.../block0.rng. Verify it matches your file.
        std::cerr << "Attempting to load A2 from its compiled-in path...\n";

        // --- Load full dataset ---
        datasets::A2 a2; // uses compiled-in path in a2.hpp
        const std::size_t N = a2.size();
        std::cout << "A2 loaded items: " << N << " (each 4 bytes)\n";

        // Basic checksum on the raw buffer
        const auto& raw = a2.buffer();
        const std::uint32_t sum32 = fnv1a32(raw.data(), raw.size());
        std::cout << "A2 raw byte FNV1a32 checksum: 0x" << std::hex << sum32 << std::dec << "\n";

        // --- Walk the stream and confirm item count & gather stats ---
        auto s = a2.make_stream();
        const void* p; std::size_t len;
        std::size_t seen = 0;
        std::unordered_set<std::uint32_t> distinct;
        distinct.reserve(N / 2 + 1024);

        // small histogram over low 8 bits (sanity uniformity check)
        std::uint64_t bucket[256]; std::memset(bucket, 0, sizeof(bucket));

        while (s.next(p, len)) {
            if (len != 4) { std::cerr << "ERROR: item length != 4\n"; return 2; }
            ++seen;
            std::uint32_t k = load_le_u32(p);
            distinct.insert(k);
            bucket[std::uint8_t(k & 0xFF)]++;
        }
        std::cout << "Stream iterated items: " << seen << "\n";
        if (seen != N) {
            std::cerr << "ERROR: seen!=N (" << seen << " vs " << N << ")\n";
            return 2;
        }

        std::cout << "Distinct keys: " << distinct.size() << " (out of " << N << " items)\n";

        // print a tiny summary of low-8-bit buckets
        std::uint64_t bmin = bucket[0], bmax = bucket[0];
        for (int i = 1; i < 256; ++i) { if (bucket[i] < bmin) bmin = bucket[i]; if (bucket[i] > bmax) bmax = bucket[i]; }
        std::cout << "Low-8-bit bucket counts: min=" << bmin << " max=" << bmax
            << " (ideal mean~" << (N / 256.0) << ")\n";

        // --- Determinism check (reloading should produce identical bytes) ---
        datasets::A2 a2b; // load again
        if (a2b.size() != N || a2b.buffer() != raw) {
            std::cerr << "ERROR: Reloaded A2 differs from first load.\n";
            return 3;
        }
        std::cout << "Determinism OK (reload identical).\n";

        // --- 50/50 position-based split and Jaccard on distincts ---
        datasets::A2Split split; // uses same compiled-in path inside A2
        auto sa = split.make_streamA();
        auto sb = split.make_streamB();

        std::unordered_set<std::uint32_t> Aset; Aset.reserve(N / 4 + 1024);
        std::unordered_set<std::uint32_t> Bset; Bset.reserve(N / 4 + 1024);

        while (sa.next(p, len)) { std::uint32_t k = load_le_u32(p); Aset.insert(k); }
        while (sb.next(p, len)) { std::uint32_t k = load_le_u32(p); Bset.insert(k); }

        // compute Jaccard on distincts
        std::size_t inter = 0;
        if (Aset.size() < Bset.size()) {
            for (auto k : Aset) inter += Bset.count(k);
        }
        else {
            for (auto k : Bset) inter += Aset.count(k);
        }
        std::size_t uni = Aset.size() + Bset.size() - inter;
        double jacc = (uni ? double(inter) / double(uni) : 1.0);

        std::cout << "Split A distinct=" << Aset.size()
            << "  Split B distinct=" << Bset.size()
            << "  Union=" << uni
            << "  Intersection=" << inter
            << "  Jaccard=" << jacc << "\n";

        std::cout << "A2 verify: OK.\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
#if __has_include(<filesystem>)
        std::cerr << "cwd: " << std::filesystem::current_path().string() << "\n";
#endif
        return 1;
    }
}
