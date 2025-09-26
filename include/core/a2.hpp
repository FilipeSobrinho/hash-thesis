#pragma once
// datasets/a2.hpp
// A2: 100k-sample-with-replacement from the first 100k 32-bit items of block0.mg.
// - Read the first POOL_N = 100'000 keys (4B each, little-endian) from file.
// - Form a STREAM_N = 100'000-length stream by sampling *with replacement*
//   uniformly from that pool.
// - Fully materialized; same usage as A1: make_stream() -> StreamPtr (len==4).
// - Provides a 50/50 position-based split for OPH via A2Split.
//


#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>
#include <random>

#include "a1.hpp" // for datasets::StreamPtr (same API as A1)

#ifndef ROOT_DEFAULT_DIR
// Define the default seed directory if not defined by build system
#define ROOT_DEFAULT_DIR "."
#endif


namespace datasets {
    const std::string& consRootDir = ROOT_DEFAULT_DIR;
    std::string rootDir = consRootDir;
    const std::string& filepath = rootDir += "/block0.rng";


    // Deterministic PRNG seed for sampling (change if desired)
    static constexpr std::uint64_t A2_SAMPLE_SEED = 0xA2A2A2A2DEADBEEFull;

    static inline std::uint64_t a2_splitmix64(std::uint64_t x) {
        x += 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        return x ^ (x >> 31);
    }

    class A2 {
    public:
        // Construct A2 from `filepath` (defaults to "block0.mg" in CWD).
        // Pool = first 100k items; Stream = 100k samples with replacement from pool.
        explicit A2() {
            build_from_file(filepath);
        }

        std::size_t size() const { return N_; }               // number of 4B items in stream
        const std::uint8_t* data() const { return buf_.data(); }
        const std::vector<std::uint8_t>& buffer() const { return buf_; }

        // Same iterator API as A1
        StreamPtr make_stream() const { return StreamPtr(buf_.data(), N_); }

    private:
        static constexpr std::size_t POOL_N = 250'000;  // first 100k from file
        static constexpr std::size_t STREAM_N = 500'000;  // sample 100k with replacement

        void build_from_file(const std::string& path) {
            std::ifstream in(path, std::ios::binary);
            if (!in) throw std::runtime_error("A2: cannot open file: " + path);

            // Read up to POOL_N * 4 bytes (first 100k items). If file is shorter
            // we take as many full items as available (must be >= 1 item).
            in.seekg(0, std::ios::end);
            const std::uint64_t fsz = static_cast<std::uint64_t>(in.tellg());
            if (static_cast<std::int64_t>(fsz) < 0) throw std::runtime_error("A2: tellg failed: " + path);
            in.seekg(0, std::ios::beg);

            std::size_t available_items = static_cast<std::size_t>(fsz / 4u);
            if (available_items == 0) throw std::runtime_error("A2: file has fewer than 4 bytes: " + path);

            const std::size_t pool_items = (available_items < POOL_N) ? available_items : POOL_N;
            const std::size_t pool_bytes = pool_items * 4;

            pool_.resize(pool_bytes);
            if (!in.read(reinterpret_cast<char*>(pool_.data()), static_cast<std::streamsize>(pool_bytes))) {
                throw std::runtime_error("A2: failed to read pool bytes from: " + path);
            }

            // Sample STREAM_N items with replacement from [0 .. pool_items-1]
            buf_.resize(STREAM_N * 4);
            std::mt19937_64 gen(A2_SAMPLE_SEED);
            std::uniform_int_distribution<std::size_t> pick(0, pool_items - 1);

            std::uint8_t* out = buf_.data();
            for (std::size_t i = 0; i < STREAM_N; ++i) {
                const std::size_t idx = pick(gen);
                const std::uint8_t* src = pool_.data() + idx * 4;
                // copy 4 bytes (little-endian key)
                out[0] = src[0];
                out[1] = src[1];
                out[2] = src[2];
                out[3] = src[3];
                out += 4;
            }
            N_ = STREAM_N;
        }

        std::size_t N_ = 0;                 // number of 4-byte items in the *sampled* stream
        std::vector<std::uint8_t> pool_;    // first 100k items from file (4B each)
        std::vector<std::uint8_t> buf_;     // sampled stream (4B each)
    };

    // 50/50 position-based split over the *sampled* stream.
    class A2Split {
    public:
        explicit A2Split(std::uint64_t split_seed = 0xA5A5A5A5A5A5A5A5ull)
        {
            A2 base;
            split_into_groups(base.buffer(), split_seed);
        }

        std::size_t sizeA() const { return A_items_; }
        std::size_t sizeB() const { return B_items_; }

        const std::vector<std::uint8_t>& bufferA() const { return A_buf_; }
        const std::vector<std::uint8_t>& bufferB() const { return B_buf_; }

        StreamPtr make_streamA() const { return StreamPtr(A_buf_.data(), A_items_); }
        StreamPtr make_streamB() const { return StreamPtr(B_buf_.data(), B_items_); }

    private:
        void split_into_groups(const std::vector<std::uint8_t>& sampled, std::uint64_t seed) {
            A_buf_.clear(); B_buf_.clear();
            A_buf_.reserve(sampled.size() / 2 + 64);
            B_buf_.reserve(sampled.size() / 2 + 64);

            const std::size_t items = sampled.size() / 4;
            for (std::size_t idx = 0; idx < items; ++idx) {
                const std::uint64_t g = a2_splitmix64(seed + idx) & 1ull; // 0 or 1
                const std::uint8_t* src = sampled.data() + idx * 4;
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

        std::vector<std::uint8_t> A_buf_, B_buf_;
        std::size_t A_items_ = 0, B_items_ = 0;
    };

} // namespace datasets
