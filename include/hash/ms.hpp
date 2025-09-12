#pragma once
// Multiply-Shift with 64-bit a,b; 32-bit input; returns high 32 bits.
// h(x) = upper_32_bits( a * x + b ), with a forced odd.

#include <cstdint>

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

namespace hashfn {

    struct MS {
        MS() = default;

        // Set 64-bit parameters. 'a' is forced odd to match common practice.
        HASH_FORCEINLINE void set_params(std::uint64_t a, std::uint64_t b) {
            a_ = a | 1ull;  // ensure odd
            b_ = b;
        }

        // Hash a 32-bit input -> return the high 32 bits of (a*x + b) mod 2^64.
        HASH_FORCEINLINE std::uint32_t hash(std::uint32_t x) const {
            std::uint64_t y = a_ * static_cast<std::uint64_t>(x) + b_; // wraps mod 2^64
            return static_cast<std::uint32_t>(y >> 32);
        }

        HASH_FORCEINLINE std::uint64_t a() const { return a_; }
        HASH_FORCEINLINE std::uint64_t b() const { return b_; }

    private:
        std::uint64_t a_{ 1 };   // must be odd; we enforce in set_params
        std::uint64_t b_{ 0 };
    };

} // namespace hashfn
