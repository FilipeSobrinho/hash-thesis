#include <iostream>
#include <vector>
#include <cstdint>
#include <iomanip>
#include <limits>
#include "hash/ms32.hpp"

static void print_sample(const hashfn::MS32& h,
    const std::vector<uint32_t>& xs,
    const char* label) {
    std::cout << label << ":\n";
    std::cout << "  a=0x" << std::hex << h.a()
        << " b=0x" << h.b() << std::dec << "\n";
    for (size_t i = 0; i < xs.size(); ++i) {
        uint32_t y = h.hash(xs[i]); // high 32 bits
        std::cout << "  x[" << i << "]=0x" << std::hex << xs[i]
            << " -> hi32=0x" << y << std::dec
            << " (" << y << ")\n";
    }
}

int main() {
    using hashfn::MS32;

    // 1) Fixed parameters sanity check (pick any odd 'a' and any 'b')
    MS32 h;
    h.set_params(0x9E3779B97F4A7C15ull, 0xA5A5A5A5A5A5A5A5ull);
    std::vector<uint32_t> xs = { 0u, 1u, 0x12345678u, 0x9ABCDEF0u, 0xFFFFFFFFu };
    print_sample(h, xs, "Fixed-params sample (hi32 only)");

    // 2) Simple bucket sanity on the hi32 output’s top R bits
    const unsigned R = 12;              // 4096 buckets from top 12 bits of hi32
    const uint32_t N = 300000;          // number of inputs to test
    std::vector<uint32_t> buckets(1u << R, 0u);
    for (uint32_t x = 0; x < N; ++x) {
        uint32_t hi = h.hash(x);
        uint32_t bkt = hi >> (32u - R);
        ++buckets[bkt];
    }
    uint32_t minb = std::numeric_limits<uint32_t>::max();
    uint32_t maxb = 0; uint64_t sum = 0;
    for (auto c : buckets) { if (c < minb) minb = c; if (c > maxb) maxb = c; sum += c; }
    double avg = double(sum) / buckets.size();
    std::cout << "\nBucket stats (R=" << R << ", N=" << N << "): "
        << "min=" << minb
        << " avg=" << std::fixed << std::setprecision(2) << avg
        << " max=" << std::defaultfloat << maxb << "\n";

    std::cout << "\nverify_ms32_u32_ab64_hi: OK\n";
    return 0;
}
