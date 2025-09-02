#include <iostream>
#include <vector>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include "core/randomgen.hpp"   

// Helper: read all .bin files (lexicographic) from RNG_DEFAULT_SEED_DIR into one buffer
static std::vector<uint8_t> read_seed_bytes_from_repo() {
    const std::filesystem::path dir = std::filesystem::path(RNG_DEFAULT_SEED_DIR);
    if (!std::filesystem::exists(dir)) {
        throw std::runtime_error("Seed dir does not exist: " + dir.string());
    }
    std::vector<std::filesystem::path> files;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.is_regular_file() && e.path().extension() == ".bin")
            files.push_back(e.path());
    }
    std::sort(files.begin(), files.end());
    if (files.empty()) {
        throw std::runtime_error("No .bin files found in: " + dir.string());
    }

    std::vector<uint8_t> buf;
    for (const auto& p : files) {
        std::ifstream in(p, std::ios::binary);
        if (!in) throw std::runtime_error("Cannot open: " + p.string());
        in.seekg(0, std::ios::end);
        std::streamsize sz = in.tellg();
        in.seekg(0, std::ios::beg);
        if (sz <= 0) continue;
        const size_t off = buf.size();
        buf.resize(off + static_cast<size_t>(sz));
        in.read(reinterpret_cast<char*>(buf.data() + off), sz);
    }
    if (buf.empty()) throw std::runtime_error("Total seed bytes read = 0");
    return buf;
}

int main() {
    try {
        // 0) Load reference bytes directly from repo and set up test sizes
        auto ref = read_seed_bytes_from_repo();
        const size_t need = 1/*k u8s*/ + 1/*byte for 8 bools*/ + 4/*u32*/ + 8/*u64*/;
        if (ref.size() < need + 16) {
            std::cerr << "Seed pool too small for this verify. Need at least "
                << (need + 16) << " bytes, have " << ref.size() << ".\n";
            return 1;
        }

        // 1) Draw K bytes via rng::get_u8() and compare to reference
        const size_t K = 16; // first 16 bytes
        std::vector<uint8_t> got;
        got.reserve(K);
        for (size_t i = 0; i < K; ++i) got.push_back(rng::get_u8());

        bool ok = true;
        for (size_t i = 0; i < K; ++i) {
            if (got[i] != ref[i]) {
                std::cerr << "[FAIL] u8 mismatch at i=" << i
                    << " got=0x" << std::hex << (unsigned)got[i]
                    << " exp=0x" << (unsigned)ref[i] << std::dec << "\n";
                ok = false;
                break;
            }
        }
        if (!ok) return 2;
        std::cout << "u8 check OK for first " << K << " bytes.\n";

        // 2) Now test boolean packing:
        //    Next 8 booleans should reconstruct the next byte (LSB-first as implemented).
        uint8_t expect_next_byte = ref[K]; // byte at offset K
        uint8_t rebuilt = 0;
        for (int bit = 0; bit < 8; ++bit) {
            bool b = rng::get_bool();       // consumes bits from the next seed byte
            rebuilt |= (uint8_t(b) << bit); // LSB-first
        }
        if (rebuilt != expect_next_byte) {
            std::cerr << "[FAIL] bool packing mismatch: rebuilt=0x" << std::hex << (unsigned)rebuilt
                << " exp=0x" << (unsigned)expect_next_byte << std::dec << "\n";
            return 3;
        }
        std::cout << "bool packing check OK.\n";

        // 3) Test u32 assembly (big-endian) from subsequent 4 bytes on disk
        // After K u8's and 1 bool-byte, the next 4 bytes start at offset K+1.
        auto be32 = [&](size_t off)->uint32_t {
            return (uint32_t(ref[off + 0]) << 24) |
                (uint32_t(ref[off + 1]) << 16) |
                (uint32_t(ref[off + 2]) << 8) |
                (uint32_t(ref[off + 3]) << 0);
            };
        const size_t off_u32 = K + 1;
        uint32_t got32 = rng::get_u32();
        uint32_t exp32 = be32(off_u32);
        if (got32 != exp32) {
            std::cerr << "[FAIL] u32 mismatch: got=0x" << std::hex << got32
                << " exp=0x" << exp32 << std::dec << "\n";
            return 4;
        }
        std::cout << "u32 check OK.\n";

        // 4) Test u64 assembly (big-endian) from subsequent 8 bytes on disk
        // Next 8 bytes start at offset off_u32 + 4.
        auto be64 = [&](size_t off)->uint64_t {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) v = (v << 8) | uint64_t(ref[off + i]);
            return v;
            };
        const size_t off_u64 = off_u32 + 4;
        uint64_t got64 = rng::get_u64();
        uint64_t exp64 = be64(off_u64);
        if (got64 != exp64) {
            std::cerr << "[FAIL] u64 mismatch: got=0x" << std::hex << got64
                << " exp=0x" << exp64 << std::dec << "\n";
            return 5;
        }
        std::cout << "u64 check OK.\n";



        // Pretty print a small sample for human eyeballing
        std::cout << "\nSample (first 8 bytes from rng): ";
        std::cout << std::hex;
        for (int i = 0; i < 8; ++i) std::cout << " " << std::setw(2) << std::setfill('0') << (unsigned)got[i];
        std::cout << std::dec << "\n";

        std::cout << "\nverify_randomgen (fixed-path) : OK\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "verify_randomgen: exception: " << e.what() << "\n";
        return 100;
    }
}
