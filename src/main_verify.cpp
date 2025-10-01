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

#include "core/r2.hpp"

// 32-bit FNV-1a over arbitrary bytes
static inline std::uint32_t fnv1a32(const std::uint8_t* data, std::size_t nbytes) {
    constexpr std::uint32_t FNV_PRIME = 16777619u;
    std::uint32_t h = 2166136261u;
    for (std::size_t i = 0; i < nbytes; ++i) { h ^= data[i]; h *= FNV_PRIME; }
    return h;
}

// A small view wrapper for distinct counting (hash by FNV fold)
struct View {
    const std::uint8_t* p;
    std::uint32_t len;
};
struct ViewHash {
    std::size_t operator()(const View& v) const noexcept {
        return (std::size_t)fnv1a32(v.p, v.len);
    }
};
struct ViewEq {
    bool operator()(const View& a, const View& b) const noexcept {
        if (a.len != b.len) return false;
        return std::memcmp(a.p, b.p, a.len) == 0;
    }
};

int main(int argc, char** argv) {
    try {
#if __has_include(<filesystem>)
        std::cerr << "cwd: " << std::filesystem::current_path().string() << "\n";
#endif

        std::string inpath = std::string(ROOT_DEFAULT_DIR) + "/" + datasets::R2_DEFAULT_FILE;
        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            if (a == "--file" && i + 1 < argc) inpath = argv[++i];
            else if (a == "--help" || a == "-h") {
                std::cout << "Usage: verify_r2 [--file path_to_words.txt]\n"; return 0;
            }
        }

        // Load full R2 (first 1e5 words)
        datasets::R2 r2(inpath);
        auto s = r2.make_stream();

        const auto& buf = r2.buffer();
        std::cout << "R2 loaded items: " << r2.size() << " (variable-length words)\n";
        std::cout << "R2 buffer bytes : " << buf.size() << "\n";
        std::cout << "R2 FNV1a32 checksum of buffer: 0x" << std::hex
            << fnv1a32(buf.data(), buf.size()) << std::dec << "\n";

        // Iterate and count distinct
        const void* p; std::size_t len;
        std::unordered_set<View, ViewHash, ViewEq> distinct;
        distinct.reserve(r2.size());
        std::size_t seen = 0;
        while (s.next(p, len)) {
            ++seen;
            View v{ (const std::uint8_t*)p, static_cast<std::uint32_t>(len) };
            distinct.insert(v);
        }
        std::cout << "Stream iterated items: " << seen << "\n";
        std::cout << "Distinct words: " << distinct.size() << "\n";

        // Determinism check (rebuild and compare byte-for-byte)
        datasets::R2 r2b(inpath);
        if (r2b.buffer() != buf || r2b.size() != r2.size()) {
            std::cerr << "ERROR: R2 reload differs from first load.\n"; return 3;
        }
        std::cout << "Determinism OK (reload identical).\n";

        // Split: first half vs second half (position-based)
        datasets::R2Split split(inpath);
        std::cout << "Split sizes: A=" << split.sizeA() << " B=" << split.sizeB()
            << " (sum=" << (split.sizeA() + split.sizeB()) << ")\n";

        // Jaccard on distinct words of A vs B
        auto jaccard = [](auto&& Astream, auto&& Bstream)->double {
            const void* p; std::size_t len;
            std::unordered_set<View, ViewHash, ViewEq> Aset, Bset;
            while (Astream.next(p, len)) Aset.insert(View{ (const std::uint8_t*)p,(std::uint32_t)len });
            while (Bstream.next(p, len)) Bset.insert(View{ (const std::uint8_t*)p,(std::uint32_t)len });
            std::size_t inter = 0;
            if (Aset.size() < Bset.size()) { for (auto& v : Aset) inter += Bset.count(v); }
            else { for (auto& v : Bset) inter += Aset.count(v); }
            const std::size_t uni = Aset.size() + Bset.size() - inter;
            return uni ? double(inter) / double(uni) : 1.0;
            };

        auto sa = split.make_streamA();
        auto sb = split.make_streamB();
        const double J = jaccard(sa, sb);
        std::cout << "Split Jaccard (distinct words): " << J << "\n";

        std::cout << "R2 verify: OK.\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
}
