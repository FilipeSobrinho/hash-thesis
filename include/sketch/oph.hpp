#pragma once

#include <cstdint>
#include <vector>
#include <limits>
#include <algorithm>
#include <stdexcept>

#ifndef HASH_FORCEINLINE
#if defined(_MSC_VER)
#define HASH_FORCEINLINE __forceinline
#elif defined(__clang__) || defined(__GNUC__)
#define HASH_FORCEINLINE inline __attribute__((always_inline))
#else
#define HASH_FORCEINLINE inline
#endif
#endif

namespace sketch {

    // One-Permutation Hashing (no densification).
    // Keep the minimum hash per bin; empty bins are 0xFFFFFFFF.
    // Indexing uses a fast, unbiased reducer: floor((hv * m) / 2^32).
    class OPH {
    public:
        static constexpr std::uint32_t EMPTY = 0xFFFFFFFFu;

        explicit OPH(std::uint32_t m_bins)
            : m_(m_bins), bins_(m_bins, EMPTY)
        {
            if (m_ == 0) throw std::invalid_argument("OPH: m_bins must be > 0");
        }

        HASH_FORCEINLINE void clear() {
            std::fill(bins_.begin(), bins_.end(), EMPTY);
        }

        // Insert a 32-bit hash value.
        HASH_FORCEINLINE void push(std::uint32_t hv) {
            const std::uint32_t i = fast_range32(hv, m_);
            std::uint32_t& slot = bins_[i];
            if (hv < slot) slot = hv;
        }

        // Access
        HASH_FORCEINLINE std::uint32_t m() const { return m_; }
        HASH_FORCEINLINE const std::vector<std::uint32_t>& bins() const { return bins_; }
        HASH_FORCEINLINE std::vector<std::uint32_t>& bins_mut() { return bins_; }

    private:
        std::uint32_t m_;
        std::vector<std::uint32_t> bins_;

        // fast, unbiased reduction to [0, n) using 32x32->64 multiply
        static HASH_FORCEINLINE std::uint32_t fast_range32(std::uint32_t x, std::uint32_t n) {
            return std::uint32_t((std::uint64_t(x) * std::uint64_t(n)) >> 32);
        }
    };

    // Jaccard estimate between two OPH sketches (no densification):
    // matches = count of bins where both are non-empty and equal
    // denom   = count of bins where at least one is non-empty
    // If denom==0 (both sets empty), return 1.0.
    inline double jaccard(const OPH& A, const OPH& B) {
        if (A.m() != B.m()) throw std::invalid_argument("OPH jaccard: mismatched m");
        const auto& a = A.bins();
        const auto& b = B.bins();

        std::uint64_t matches = 0, denom = 0;
        for (std::size_t i = 0; i < a.size(); ++i) {
            const bool ae = (a[i] == OPH::EMPTY);
            const bool be = (b[i] == OPH::EMPTY);
            if (ae && be) continue;          // both empty -> ignore
            ++denom;                         // at least one non-empty
            if (!ae && !be && a[i] == b[i]) ++matches;
        }
        if (denom == 0) return 1.0;       // both sets empty
        return double(matches) / double(denom);
    }

} // namespace sketch
