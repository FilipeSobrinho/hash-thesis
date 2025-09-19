#pragma once
// Count-Min Sketch (32-bit keys). Simple & efficient.
// - d rows (default 3), each with its own hash function.
// - width W counters per row (uint32_t).
// - add(key, c=1), estimate(key) = min over rows.
//
// Register hashers per row with set_row(row_index, hasher_ptr).
// The hasher must have:  uint32_t hash(uint32_t key) const;
//
// Example:
//   sketch::CountMin cms(/*width=*/1<<20, /*depth=*/3);
//   cms.set_row(0, &h_ms);
//   cms.set_row(1, &h_tab);
//   cms.set_row(2, &h_tornado);
//   cms.add(123u);
//   auto est = cms.estimate(123u);

#include <cstdint>
#include <cstddef>
#include <vector>
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

    class CountMin {
    public:
        using counter_t = std::uint32_t;

        explicit CountMin(std::size_t width, std::size_t depth = 3)
            : W_(width), D_(depth), rows_(depth), table_(width* depth, 0)
        {
            if (W_ == 0 || D_ == 0) throw std::invalid_argument("CountMin: width and depth must be > 0");
        }

        // Register a hasher for a given row.
        // Hasher type H must have:  uint32_t hash(uint32_t) const;
        template <class H>
        HASH_FORCEINLINE void set_row(std::size_t row, const H* hasher_ptr) {
            if (row >= D_) throw std::out_of_range("CountMin::set_row: row out of range");
            rows_[row].ctx = const_cast<void*>(static_cast<const void*>(hasher_ptr));
            rows_[row].fn = [](void* c, std::uint32_t key) -> std::uint32_t {
                const H* h = static_cast<const H*>(c);
                return h->hash(key);
                };
        }

        HASH_FORCEINLINE void clear() {
            std::fill(table_.begin(), table_.end(), counter_t(0));
        }

        // Add 'count' to the key's counters (default 1). Saturates at UINT32_MAX.
        HASH_FORCEINLINE void add(std::uint32_t key, std::uint32_t count = 1) {
            for (std::size_t r = 0; r < D_; ++r) {
                const std::uint32_t hv = rows_[r].hash(key);
                const std::size_t col = fast_range32(hv, static_cast<std::uint32_t>(W_));
                counter_t& cell = table_[r * W_ + col];
                const std::uint64_t sum = std::uint64_t(cell) + count;
                cell = (sum > UINT32_MAX) ? UINT32_MAX : static_cast<counter_t>(sum);
            }
        }

        // Point query (estimate frequency of 'key'): min over rows.
        HASH_FORCEINLINE std::uint32_t estimate(std::uint32_t key) const {
            std::uint32_t ans = UINT32_MAX;
            for (std::size_t r = 0; r < D_; ++r) {
                const std::uint32_t hv = rows_[r].hash(key);
                const std::size_t col = fast_range32(hv, static_cast<std::uint32_t>(W_));
                const counter_t v = table_[r * W_ + col];
                if (v < ans) ans = v;
            }
            return ans;
        }

        // Accessors
        HASH_FORCEINLINE std::size_t width() const { return W_; }
        HASH_FORCEINLINE std::size_t depth() const { return D_; }
        HASH_FORCEINLINE const std::vector<counter_t>& table() const { return table_; }
        HASH_FORCEINLINE std::vector<counter_t>& table_mut() { return table_; }

    private:
        struct Row {
            void* ctx = nullptr;
            std::uint32_t(*fn)(void*, std::uint32_t) = nullptr;
            HASH_FORCEINLINE std::uint32_t hash(std::uint32_t k) const {
                // If a row wasn't set, fall back to identity (debug-friendly)
                return (fn ? fn(ctx, k) : k);
            }
        };

        static HASH_FORCEINLINE std::uint32_t fast_range32(std::uint32_t x, std::uint32_t n) {
            return std::uint32_t((std::uint64_t(x) * std::uint64_t(n)) >> 32);
        }

        std::size_t W_, D_;
        std::vector<Row> rows_;          // D rows
        std::vector<counter_t> table_;   // size = D * W
    };

} // namespace sketch
