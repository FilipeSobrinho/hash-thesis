#include <cstdint>
#include <cstdio>
#include <cstring>
#include <inttypes.h>
#include <vector>
#include "hash/rapidhash.h"

static bool check_top32_matches_shift() {
    const char* msgs[] = { "", "a", "abc", "message digest",
                          "abcdefghijklmnopqrstuvwxyz",
                          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
                          "123456789012345678901234567890123456789012345678901234567890"
                          "12345678901234567890" };

    bool ok = true;
    for (auto s : msgs) {
        const size_t n = std::strlen(s);
        const uint64_t h64 = rapidhash(s, n);
        const uint32_t want = static_cast<uint32_t>(h64 >> 32);
        const uint32_t got = rapidhash32(s, n);
        if (got != want) {
            std::printf("FAIL: top32 mismatch for \"%s\": 0x%08" PRIx32 " vs 0x%08" PRIx32 "\n",
                s, got, want);
            ok = false;
        }
    }
    if (ok) std::puts("OK: rapidhash32 == top 32 bits of rapidhash.");
    return ok;
}

static bool check_wrapper32_vs_internal() {
    rapid::RapidHash32 H;
    H.set_params(/*seed*/ 0x0123456789ABCDEFull, rapid_secret[0], rapid_secret[1], rapid_secret[2]);

    const char* msgs[] = { "", "a", "abc", "abcdefgh", "abcdefghABCDEFGH", "0123456789abcdefghij" };
    bool ok = true;
    for (auto s : msgs) {
        const size_t n = std::strlen(s);
        const uint32_t a = H.hash(s, n);
        const uint32_t b = rapidhash32_withSeed(s, n, 0x0123456789ABCDEFull);
        if (a != b) {
            std::printf("FAIL: wrapper32 vs withSeed mismatch for \"%s\": a=0x%08" PRIx32 " b=0x%08" PRIx32 "\n",
                s, a, b);
            ok = false;
        }
    }
    if (ok) std::puts("OK: RapidHash32 wrapper matches rapidhash32_withSeed.");
    return ok;
}

int main() {
    bool ok1 = check_top32_matches_shift();
    bool ok2 = check_wrapper32_vs_internal();
    if (ok1 && ok2) {
        std::puts("\nverify_rapidhash32: ALL GOOD");
        return 0;
    }
    return 1;
}
