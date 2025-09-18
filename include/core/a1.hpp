#pragma once
// datasets/a1.hpp
// A1 dataset with Jaccard aprox = 0.5 under a 50/50 position-based split.
// - First half: pairs (two copies per distinct key)
// - Second half: skewed tail with w(i) = ceil(i/100) repeats for key i
// Fully materialized; 32-bit little-endian keys.
//
// Provides:
//   datasets::A1            -> whole stream
//   datasets::A1Split       -> two streams (A,B) from a fixed 50/50 split
//   StreamPtr               -> next(const void*& ptr, size_t& len), len==4

#include <cstdint>
#include <cstddef>
#include <vector>
#include <algorithm>
#include <stdexcept>

namespace datasets {

    // ---------- helpers ----------
    static inline void store_le_u32(void* dst, std::uint32_t v) {
        std::uint8_t* b = static_cast<std::uint8_t*>(dst);
        b[0] = std::uint8_t(v & 0xFF);
        b[1] = std::uint8_t((v >> 8) & 0xFF);
        b[2] = std::uint8_t((v >> 16) & 0xFF);
        b[3] = std::uint8_t((v >> 24) & 0xFF);
    }

    static inline std::uint32_t a1_repeats(std::uint32_t key) {
        // ceil(key / 100)
        return (key + 99u) / 100u;
    }

    static inline std::uint64_t splitmix64(std::uint64_t x) {
        x += 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        return x ^ (x >> 31);
    }

    // ---------- pointer-size block stream over a contiguous 4B-key buffer ----------
    class StreamPtr {
    public:
        StreamPtr() = default;
        StreamPtr(const std::uint8_t* base, std::size_t n_items)
            : base_(base), n_(n_items) {
        }

        void reset(const std::uint8_t* base, std::size_t n_items) {
            base_ = base; n_ = n_items; i_ = 0;
        }

        // Returns false when exhausted; each record is a 4-byte LE key.
        bool next(const void*& out_ptr, std::size_t& out_len) {
            if (i_ >= n_) return false;
            out_ptr = base_ + (i_ * 4);
            out_len = 4;
            ++i_;
            return true;
        }

        void rewind() { i_ = 0; }
        std::size_t size_hint() const { return n_; }

    private:
        const std::uint8_t* base_ = nullptr;
        std::size_t n_ = 0;
        std::size_t i_ = 0;
    };

    // ---------- A1: whole stream materialized (paired first half + skewed second half) ----------
    class A1 {
    public:
        explicit A1(std::size_t N) : N_(N), buf_(N * 4) {
            if (N_ == 0) throw std::invalid_argument("A1: N must be > 0");
            fill_buffer();
        }

        std::size_t size() const { return N_; }
        const std::uint8_t* data() const { return buf_.data(); }
        const std::vector<std::uint8_t>& buffer() const { return buf_; }

        StreamPtr make_stream() const { return StreamPtr(buf_.data(), N_); }

    private:
        void fill_buffer() {
            const std::size_t half = N_ / 2;

            // --- First half: pairs (two copies per key) to induce overlap under 50/50 split ---
            {
                std::size_t pos = 0;
                std::uint32_t v = 1;
                // write (v, v), (v+1, v+1), ...
                while (pos + 1 < half) {
                    store_le_u32((void*)(buf_.data() + (pos + 0) * 4), v);
                    store_le_u32((void*)(buf_.data() + (pos + 1) * 4), v);
                    pos += 2;
                    ++v;
                }
                if (pos < half) {
                    // odd slot leftover: one copy of v
                    store_le_u32((void*)(buf_.data() + pos * 4), v);
                    ++pos;
                    ++v;
                }
            }

            // --- Second half: skewed tail with w(i) = ceil(i/100) repeats ---
            {
                std::uint32_t key = 1;
                std::uint32_t rep = 0;
                for (std::size_t p = half; p < N_; ++p) {
                    store_le_u32((void*)(buf_.data() + p * 4), key);
                    if (++rep >= a1_repeats(key)) {
                        rep = 0;
                        ++key;
                    }
                }
            }
        }

        std::size_t N_;
        std::vector<std::uint8_t> buf_;
    };

    // ---------- A1Split: build ONE 50/50 split (position-random) into A/B buffers ----------
    class A1Split {
    public:
        // Build a fixed split of A1(N) using split_seed. The same split is reused.
        A1Split(std::size_t N, std::uint64_t split_seed)
            : N_(N)
        {
            if (N_ == 0) throw std::invalid_argument("A1Split: N must be > 0");
            A1 base(N_);
            split_into_groups(base.buffer(), split_seed);
        }

        std::size_t sizeA() const { return A_items_; }
        std::size_t sizeB() const { return B_items_; }

        const std::vector<std::uint8_t>& bufferA() const { return A_buf_; }
        const std::vector<std::uint8_t>& bufferB() const { return B_buf_; }

        StreamPtr make_streamA() const { return StreamPtr(A_buf_.data(), A_items_); }
        StreamPtr make_streamB() const { return StreamPtr(B_buf_.data(), B_items_); }

    private:
        void split_into_groups(const std::vector<std::uint8_t>& base, std::uint64_t seed) {
            A_buf_.clear(); B_buf_.clear();
            A_buf_.reserve(base.size() / 2 + 64);
            B_buf_.reserve(base.size() / 2 + 64);

            const std::size_t items = base.size() / 4;
            for (std::size_t idx = 0; idx < items; ++idx) {
                const std::uint64_t g = splitmix64(seed + idx) & 1ull;
                const std::uint8_t* src = base.data() + idx * 4;
                if (g == 0) {
                    A_buf_.insert(A_buf_.end(), src, src + 4);
                }
                else {
                    B_buf_.insert(B_buf_.end(), src, src + 4);
                }
            }
            A_items_ = A_buf_.size() / 4;
            B_items_ = B_buf_.size() / 4;
        }

        std::size_t N_;
        std::vector<std::uint8_t> A_buf_, B_buf_;
        std::size_t A_items_ = 0, B_items_ = 0;
    };

} // namespace datasets
