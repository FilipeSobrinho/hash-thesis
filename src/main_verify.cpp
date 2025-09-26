#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_set>
#include <iostream>

#if __has_include(<filesystem>)
#include <filesystem>
#endif

#include "core/r1.hpp"   // R1, R1Split (20-byte items)

static inline std::uint32_t fnv1a32(const std::uint8_t* data, std::size_t nbytes) {
    constexpr std::uint32_t FNV_PRIME = 16777619u;
    std::uint32_t h = 2166136261u;
    for (std::size_t i = 0; i < nbytes; ++i) {
        h ^= data[i];
        h *= FNV_PRIME;
    }
    return h;
}

// 20-byte key hasher for unordered_set
struct Key20Hash {
    std::size_t operator()(const std::array<std::uint8_t, 20>& a) const noexcept {
        // simple FNV-1a fold
        std::uint32_t h = 2166136261u;
        for (auto b : a) { h ^= b; h *= 16777619u; }
        return (std::size_t)h;
    }
};
struct Key20Eq {
    bool operator()(const std::array<std::uint8_t, 20>& x,
        const std::array<std::uint8_t, 20>& y) const noexcept {
        return std::memcmp(x.data(), y.data(), 20) == 0;
    }
};

int main(int argc, char** argv) {
    try {
#if __has_include(<filesystem>)
        std::cerr << "cwd: " << std::filesystem::current_path().string() << "\n";
#endif
        std::string inpath = "C:/Users/PCDoctor/Documents/Tese/hash-thesis/sha1_all.txt";
        bool with_repl = false; // default: without replacement if enough
        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto next = [&]() { if (i + 1 < argc) return std::string(argv[++i]); throw std::runtime_error("Missing value for " + a); };
            if (a == "--file") inpath = next();
            else if (a == "--with-replacement") with_repl = true;
        }

        // --- Load full R1 dataset (20-byte items) ---
        datasets::R1 r1;
        const std::size_t N = r1.size();
        std::cout << "R1 loaded items: " << N << " (each 20 bytes)\n";

        const auto& raw = r1.buffer();
        const std::uint32_t sum32 = fnv1a32(raw.data(), raw.size());
        std::cout << "R1 raw byte FNV1a32 checksum: 0x" << std::hex << sum32 << std::dec << "\n";

        // --- Iterate stream; confirm len==20; distinct count ---
        auto s = r1.make_stream();
        const void* p; std::size_t len;
        std::size_t seen = 0;
        std::unordered_set<std::array<std::uint8_t, 20>, Key20Hash, Key20Eq> distinct;
        distinct.reserve(N * 3 / 4);

        while (s.next(p, len)) {
            if (len != 20) {
                std::cerr << "ERROR: item length != 20 (got " << len << ")\n";
                return 2;
            }
            ++seen;
            std::array<std::uint8_t, 20> key{};
            std::memcpy(key.data(), p, 20);
            distinct.insert(key);
        }
        std::cout << "Stream iterated items: " << seen << "\n";
        if (seen != N) {
            std::cerr << "ERROR: seen!=N (" << seen << " vs " << N << ")\n";
            return 2;
        }
        std::cout << "Distinct keys: " << distinct.size() << " (out of " << N << " items)\n";

        // --- Determinism check: rebuild and compare buffers ---
        datasets::R1 r1b;
        if (r1b.size() != N || r1b.buffer() != raw) {
            std::cerr << "ERROR: Reloaded R1 differs from first load.\n";
            return 3;
        }
        std::cout << "Determinism OK (reload identical).\n";

        // --- 50/50 position-based split and Jaccard on distincts ---
        datasets::R1Split split(with_repl);
        auto sa = split.make_streamA();
        auto sb = split.make_streamB();

        std::unordered_set<std::array<std::uint8_t, 20>, Key20Hash, Key20Eq> Aset, Bset;
        Aset.reserve(N / 2); Bset.reserve(N / 2);

        while (sa.next(p, len)) {
            if (len != 20) { std::cerr << "ERROR: split A item len!=20\n"; return 4; }
            std::array<std::uint8_t, 20> key{};
            std::memcpy(key.data(), p, 20);
            Aset.insert(key);
        }
        while (sb.next(p, len)) {
            if (len != 20) { std::cerr << "ERROR: split B item len!=20\n"; return 4; }
            std::array<std::uint8_t, 20> key{};
            std::memcpy(key.data(), p, 20);
            Bset.insert(key);
        }

        std::size_t inter = 0;
        if (Aset.size() < Bset.size()) {
            for (auto& k : Aset) inter += Bset.count(k);
        }
        else {
            for (auto& k : Bset) inter += Aset.count(k);
        }
        std::size_t uni = Aset.size() + Bset.size() - inter;
        double jacc = (uni ? double(inter) / double(uni) : 1.0);

        std::cout << "Split A distinct=" << Aset.size()
            << "  Split B distinct=" << Bset.size()
            << "  Union=" << uni
            << "  Intersection=" << inter
            << "  Jaccard=" << jacc << "\n";

        std::cout << "R1 verify: OK.\n";
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
