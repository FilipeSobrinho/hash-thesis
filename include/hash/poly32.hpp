#pragma once
// Based on https://github.com/kipoujr/no_repetition/blob/main/src/framework/hashing.h#L454
// Poly32: degree-d polynomial mod p = 2^61 - 1 (Carter–Wegman style).
// Deterministically seeded; emits 32-bit values for table population.

// ---- force-inline macro (local, guarded) -----------------------------------
#ifndef HASH_FORCEINLINE
#if defined(_MSC_VER)
#define HASH_FORCEINLINE __forceinline
#elif defined(__clang__) || defined(__GNUC__)
#define HASH_FORCEINLINE inline __attribute__((always_inline))
#else
#define HASH_FORCEINLINE inline
#endif
#endif
// ---------------------------------------------------------------------------

#include <cstdint>
#include <vector>
#include <cstddef>

#if defined(_MSC_VER)
// MSVC does not support __uint128_t, so use a fallback for 64x64->128 multiply
#include <intrin.h>
#endif

namespace hashfn {

    class Poly32 {
    public:
        HASH_FORCEINLINE void set_params(std::uint64_t seed, std::uint32_t degree = 100) {
            degree_ = degree ? degree : 1;
            coef_.assign(degree_, 0);
            std::uint64_t s = seed;
            for (std::uint32_t i = 0; i < degree_; ++i) {
                std::uint64_t c = splitmix64(s) >> 3;  // keep under 2^61
                c = reduce_once(c);
                if (c >= P) c -= P;
                coef_[i] = c;
            }
            if (coef_.back() == 0) coef_.back() = 1;
            x_ = 0;
        }

        HASH_FORCEINLINE std::uint32_t eval(std::uint64_t x) const {
            std::uint64_t h = coef_.back();
            for (int i = int(degree_) - 2; i >= 0; --i) {
                h = add_mod(mul_mod(h, x), coef_[std::size_t(i)]);
            }
            h = reduce_once(h);
            if (h >= P) h -= P;
            return static_cast<std::uint32_t>(h);
        }

        HASH_FORCEINLINE std::uint32_t next32() {
            return eval(x_++);
        }

    private:
        static constexpr std::uint64_t P = (1ull << 61) - 1;

        HASH_FORCEINLINE static std::uint64_t splitmix64(std::uint64_t& s) {
            std::uint64_t z = (s += 0x9E3779B97F4A7C15ull);
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
            return z ^ (z >> 31);
        }

        // (a + b) mod (2^61-1)
        HASH_FORCEINLINE static std::uint64_t add_mod(std::uint64_t a, std::uint64_t b) {
            std::uint64_t s = a + b;
            s = (s & P) + (s >> 61);
            if (s >= P) s -= P;
            return s;
        }

        // (a * b) mod (2^61-1)
        HASH_FORCEINLINE static std::uint64_t mul_mod(std::uint64_t a, std::uint64_t b) {
#if defined(__SIZEOF_INT128__)
            __uint128_t z = (__uint128_t)a * (__uint128_t)b;
            std::uint64_t lo = (std::uint64_t)z & P;
            std::uint64_t hi = (std::uint64_t)(z >> 61);
            std::uint64_t s = lo + hi;
            s = (s & P) + (s >> 61);
            if (s >= P) s -= P;
            return s;
#elif defined(_MSC_VER) && defined(_M_X64)
            // Use _umul128 for 64x64->128 multiply on MSVC x64
            unsigned __int64 hi, lo = _umul128(a, b, &hi);
            // Compose 128-bit result as z = ((uint128_t)hi << 64) | lo
            // Now reduce as above
            std::uint64_t lo61 = lo & P;
            std::uint64_t hi61 = ((hi << 3) | (lo >> 61)) & P; // hi * 2^64 mod 2^61-1
            std::uint64_t s = lo61 + hi61;
            s = (s & P) + (s >> 61);
            if (s >= P) s -= P;
            return s;
#else
#error "128-bit integer multiplication not supported on this platform/compiler"
#endif
        }

        // Single fold toward [0,P*2)
        HASH_FORCEINLINE static std::uint64_t reduce_once(std::uint64_t x) {
            return (x & P) + (x >> 61);
        }

        std::vector<std::uint64_t> coef_;
        std::uint32_t degree_{ 100 };
        std::uint64_t x_{ 0 };
    };

} // namespace hashfn
