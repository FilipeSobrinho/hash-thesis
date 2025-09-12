#pragma once
// SimpleTab32: 4-way simple tabulation on a 32-bit key.
// Table layout: T[256][4] of 32-bit words; hash is XOR of four lookups.

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
#include <array>
#include <cstddef>

namespace hashfn {

    class SimpleTab32 {
    public:
        using Table = std::array<std::array<std::uint32_t, 4>, 256>;

        HASH_FORCEINLINE void set_params(const Table& T) {
            T_ = T;
        }

        template <class Poly32Like>
        HASH_FORCEINLINE void set_params_from_poly(Poly32Like& poly) {
            for (std::size_t i = 0; i < 4; ++i) {
                for (std::size_t j = 0; j < 256; ++j) {
                    T_[j][i] = poly.next32();
                }
            }
        }

        HASH_FORCEINLINE std::uint32_t hash(std::uint32_t x) const {
            std::uint32_t h = 0;
            for (int i = 0; i < 4; ++i, x >>= 8) {
                h ^= T_[static_cast<std::uint8_t>(x)][i];
            }
            return h;
        }

    private:
        Table T_{};
    };

} // namespace hashfn
