#pragma once
// Tornado tabulation (32-bit key -> 32-bit hash) with D derived characters.
// Also provides MSVec-prehashed variants for arbitrary-length inputs.
//
// API (no operators; force-inline guarded; same shape as SimpleTab32):
//   - 32-bit input versions:
//       TornadoTab32D1..D4 : set_params(seed);  uint32_t hash(uint32_t x);
//   - Any-length input versions (MSVec -> Tornado):
//       TornadoOnMSVecD1..D4 : set_params(seed, coeffs[,force_odd=true]);
//                              uint32_t hash(const void* buf, size_t len);
//
// Dependencies: poly32.hpp, msvec.hpp, and their macros if any.

#ifndef HASH_FORCEINLINE
#if defined(_MSC_VER)
#define HASH_FORCEINLINE __forceinline
#elif defined(__clang__) || defined(__GNUC__)
#define HASH_FORCEINLINE inline __attribute__((always_inline))
#else
#define HASH_FORCEINLINE inline
#endif
#endif

#include <cstdint>
#include <array>
#include <cstddef>

#include "hash/poly.hpp"   // for table population (degree fixed to 100)
#include "hash/msvec.hpp"    // for MSVec any-length prehash (uses MSVEC_NUM_COEFFS)

namespace hashfn {

    namespace detail {

        // Base class for Tornado tabulation with D derived characters (D in [1,4]).
        template<int D>
        class TornadoTab32Base {
            static_assert(D >= 1 && D <= 4, "Derived characters D must be in [1,4]");

        public:
            // Rows: 3 for the lowest bytes + D for derived bytes
            static constexpr std::size_t ROWS = 3 + D;
            using Table = std::array<std::array<std::uint64_t, ROWS>, 256>;

            // Single setter: fill tables with Poly64(seed, degree=100)
            HASH_FORCEINLINE void set_params() {
                Poly64 poly; poly.set_params();
                for (std::size_t r = 0; r < ROWS; ++r) {
                    for (std::size_t j = 0; j < 256; ++j) {
                        T_[j][r] = poly.next64();
                    }
                }
            }

            // 32-bit key -> 32-bit hash
            HASH_FORCEINLINE std::uint32_t hash(std::uint32_t x) const {
                std::uint64_t h = 0;

                // Mix the 3 least-significant bytes via rows 0..2
                for (int i = 0; i < 3; ++i) {
                    std::uint8_t c = static_cast<std::uint8_t>(x);
                    x >>= 8;
                    h ^= T_[c][i];
                }

                // Fast-mix the most-significant byte (now x holds original MSB)
                h ^= x;

                // Derive D extra bytes from h and mix via rows 3..(2 + D)
                for (int i = 0; i < D; ++i) {
                    std::uint8_t c = static_cast<std::uint8_t>(h);
                    h >>= 8;
                    h ^= T_[c][3 + i];
                }

                return static_cast<std::uint32_t>(h);
            }

        private:
            Table T_{};
        };

    } // namespace detail

    // ---------------- Public 32-bit input variants (single set_params(seed)) ----
    class TornadoTab32D1 : public detail::TornadoTab32Base<1> {};
    class TornadoTab32D2 : public detail::TornadoTab32Base<2> {};
    class TornadoTab32D3 : public detail::TornadoTab32Base<3> {};
    class TornadoTab32D4 : public detail::TornadoTab32Base<4> {};

    // --------------- Any-length input variants (MSVec -> Tornado) ---------------
    template<int D>
    class TornadoOnMSVecBase {
    public:
        using Coeffs = std::array<std::uint64_t, MSVEC_NUM_COEFFS>;

        // Single setter:
        //  - seed   -> populates Tornado tables via Poly32(degree=100)
        //  - coeffs -> MSVec coefficients (optionally forced odd)
        HASH_FORCEINLINE void set_params(const Coeffs& coeffs,
            bool force_odd = true) {
            tornado_.set_params();
            msvec_.set_params(coeffs, force_odd);
        }

        // Any-length input -> 32-bit output
        HASH_FORCEINLINE std::uint32_t hash(const void* key, std::size_t len_bytes) const {
            const std::uint32_t mid = msvec_.hash(key, len_bytes);
            return tornado_.hash(mid);
        }

    private:
        MSVec                              msvec_;
        detail::TornadoTab32Base<D>        tornado_;
    };

    // Public non-templated wrappers (no templates at call site)
    class TornadoOnMSVecD1 : public TornadoOnMSVecBase<1> {};
    class TornadoOnMSVecD2 : public TornadoOnMSVecBase<2> {};
    class TornadoOnMSVecD3 : public TornadoOnMSVecBase<3> {};
    class TornadoOnMSVecD4 : public TornadoOnMSVecBase<4> {};

} // namespace hashfn
