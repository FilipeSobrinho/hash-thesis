#pragma once
// SimpleTab32: 4-way simple tabulation on a 32-bit key.
// Table: T[256][4] of 32-bit words; hash is XOR of four lookups.

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

#include "hash/poly.hpp"     // for internal table population (degree=100)
#include "hash/msvec.hpp"      // for TabOnMSVec (prehash any-length -> 32-bit)

// MSVEC_NUM_COEFFS is read from msvec.hpp; default is 8 unless you override.

namespace hashfn {

    class SimpleTab32 {
    public:
        using Table = std::array<std::array<std::uint32_t, 4>, 256>;

        // Single setter: populate table from Poly32 using the given seed (degree=100)
        HASH_FORCEINLINE void set_params() {
            Poly32 poly; poly.set_params();
            for (std::size_t i = 0; i < 4; ++i) {
                for (std::size_t j = 0; j < 256; ++j) {
                    T_[i][j] = poly.next32();
                }
            }
        }

        // 32-bit key -> 32-bit hash
        HASH_FORCEINLINE std::uint32_t hash(std::uint32_t x) const {
            std::uint32_t h = 0;
            for (int i = 0; i < 4; ++i, x >>= 8) {
                h ^= T_[i][static_cast<std::uint8_t>(x)];
            }
            return h;
        }

    private:
        Table T_{};
    };

    // ---------------------------------------------------------------------------
    // TabOnMSVec: arbitrary-length input hashed by MSVec, then SimpleTab32.
    // hash(buf,len) = SimpleTab32.hash( MSVec.hash(buf,len) )
    class TabOnMSVec {
    public:
        using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;

        // Single setter:
        //  - seed   -> populates SimpleTab32 via Poly32(degree=100)
        //  - coeffs -> MSVec coefficients (optionally forced odd)
        HASH_FORCEINLINE void set_params(const Coeffs& coeffs,
            bool force_odd = true)
        {
            // Set tabulation table
            stab_.set_params();
            // Set prehash coefficients
            msvec_.set_params(coeffs, force_odd);
        }

        // Any-length input -> 32-bit output
        HASH_FORCEINLINE std::uint32_t hash(const void* key, std::size_t len_bytes) const {
            const std::uint32_t mid = msvec_.hash(key, len_bytes);
            return stab_.hash(mid);
        }

    private:
        MSVec        msvec_;
        SimpleTab32  stab_;
    };

} // namespace hashfn
