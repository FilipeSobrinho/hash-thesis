#pragma once
// Bottom-k distinct counting sketch for 32-bit hash values.
// Keeps the k smallest hashes using a max-heap; estimate D aprox= (k-1) / t,
// where t is the k-th order statistic in (0,1) after normalizing by 2^32.

#include <cstdint>
#include <vector>
#include <algorithm>
#include <limits>

namespace sketch {

    class BottomK {
    public:
        explicit BottomK(std::size_t k) : k_(k) { heap_.reserve(k); }

        void clear() { heap_.clear(); }

        // Feed one 32-bit hash value.
        inline void push(std::uint32_t h) {
            if (k_ == 0) return;
            if (heap_.size() < k_) {
                heap_.push_back(h);
                if (heap_.size() == k_) std::make_heap(heap_.begin(), heap_.end()); // max-heap
                return;
            }
            // heap_.front() is the largest among the current k smallest (i.e., the k-th order statistic)
            if (h < heap_.front()) {
                std::pop_heap(heap_.begin(), heap_.end());
                heap_.back() = h;
                std::push_heap(heap_.begin(), heap_.end());
            }
        }

        std::size_t size() const { return heap_.size(); }

        // The k-th smallest (i.e., the heap's top in the max-heap)
        std::uint32_t kth_hash() const {
            return heap_.empty() ? std::numeric_limits<std::uint32_t>::max() : heap_.front();
        }

        // Cardinality estimate. If we saw fewer than k unique hashes, return that count.
        double estimate() const {
            if (heap_.size() < k_) return static_cast<double>(heap_.size());
            const std::uint32_t kmin = heap_.front();
            if (kmin == 0) return std::numeric_limits<double>::infinity(); // degenerate edge case
            constexpr double TWO32 = 4294967296.0; // 2^32
            const double t = static_cast<double>(kmin) / TWO32; // normalize to (0,1)
            return static_cast<double>(k_ - 1) / t;             // (k-1)/t
        }

    private:
        std::size_t k_;
        std::vector<std::uint32_t> heap_; // max-heap of size k
    };

} // namespace sketch
