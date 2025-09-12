#include <iostream>
#include <vector>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include "hash/msvec.hpp"

static uint32_t ref_msvec8_hi(const void* in, size_t len_bytes, const std::array<uint64_t, 8>& C) {
    const uint8_t* buf = static_cast<const uint8_t*>(in);
    const size_t   len = len_bytes / 4;
    uint64_t h = 0, t = 0;
    for (size_t i = 0; i < len; ++i, buf += 4) {
        uint32_t w; std::memcpy(&w, buf, 4);
        t = uint64_t(w) * C[i & 7u];
        h += t;
    }
    const int rem = int(len_bytes & 3u);
    if (rem) {
        uint64_t last = 0;
        if (rem & 2) { uint16_t v; std::memcpy(&v, buf, 2); last = (last << 16) | v; buf += 2; }
        if (rem & 1) { last = (last << 8) | (*buf); }
        t = last * C[len & 7u];
        h += t;
    }
    return uint32_t(h >> 32);
}

int main() {
    using hashfn::MSVec;

    // Coefficients
    std::array<uint64_t, 8> C = {
      0x9E3779B97F4A7C15ull, 0xD6E8FEB86659FD93ull,
      0xC2B2AE3D27D4EB4Full, 0x165667B19E3779F9ull,
      0x85EBCA77C2B2AE63ull, 0x27D4EB2F165667C5ull,
      0x94D049BB133111EBull, 0xBF58476D1CE4E5B9ull
    };

    MSVec H;
    H.set_params(C, /*force_odd=*/true);

    // Cases
    std::vector<uint8_t> b0;
    std::vector<uint8_t> b4 = { 1,2,3,4 };
    std::vector<uint8_t> b5 = { 1,2,3,4,5 };
    std::vector<uint8_t> b7 = { 1,2,3,4,5,6,7 };
    std::vector<uint8_t> bN(1000);
    for (size_t i = 0; i < bN.size(); ++i) bN[i] = uint8_t(i & 0xFF);

    auto check = [&](const char* name, const std::vector<uint8_t>& buf) {
        uint32_t hv = H.hash(buf.data(), buf.size());
        uint32_t rf = ref_msvec8_hi(buf.data(), buf.size(), H.coeffs());
        std::cout << std::left << std::setw(8) << name << ": " << hv << " (ref " << rf << ")\n";
        if (hv != rf) { std::cerr << "[FAIL] " << name << "\n"; std::exit(1); }
        };

    check("Empty", b0);
    check("4B", b4);
    check("5B", b5);
    check("7B", b7);
    check("1000B", bN);

    std::cout << "\nverify_msvec8_ab64_hi (set_params): OK\n";
    return 0;
}
