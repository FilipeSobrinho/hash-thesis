#pragma once
// datasets/r1.hpp
// R1: 500k-sample-with-replacement from the FIRST 250k SHA-1 hashes in a text file.
// - Input: one 40-hex SHA-1 per line (default: "sha1_all.txt").
// - POOL = first 250,000 valid lines (20 bytes each, big-endian from hex).
// - STREAM = 500,000 items sampled WITH replacement from that pool.
// - Provides a 20-byte stream (Stream20) with next(const void*&, size_t&).
// - Provides a 50/50 position-based split via R1Split.

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>
#include <random>
#include <algorithm>
#include <array>
#include <cstring>

#ifndef ROOT_DEFAULT_DIR
#define ROOT_DEFAULT_DIR "."
#endif

namespace datasets {

    // ---------- 20-byte stream over a contiguous buffer ----------
    class Stream20 {
    public:
        Stream20() = default;
        Stream20(const std::uint8_t* base, std::size_t n_items)
            : base_(base), n_(n_items) {
        }

        void reset(const std::uint8_t* base, std::size_t n_items) {
            base_ = base; n_ = n_items; i_ = 0;
        }

        // Returns false when exhausted; each record is 20 bytes.
        bool next(const void*& out_ptr, std::size_t& out_len) {
            if (i_ >= n_) return false;
            out_ptr = base_ + (i_ * 20);
            out_len = 20;
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

    // ---------- path handling ----------
    static const std::string& consRootDir = []() -> const std::string& {
        static const std::string s = ROOT_DEFAULT_DIR;
        return s;
        }();
    static std::string rootDir = consRootDir;
    static const std::string& filepath = []() -> const std::string& {
        static std::string p = rootDir + "/sha1_all.txt";
        return p;
        }();

    static constexpr std::size_t   POOL_N = 250'000;     // take first 250k
    static constexpr std::size_t   STREAM_N = 500'000;     // sample 500k
    static constexpr std::uint64_t R1_SAMPLE_SEED = 0xA55A5A55BEEFull; // deterministic

    // splitmix64 for deterministic 50/50 position-based split
    static inline std::uint64_t r1_splitmix64(std::uint64_t x) {
        x += 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        return x ^ (x >> 31);
    }

    // Convert one 40-hex string (no 0x) into 20 bytes; returns false on bad input.
    static inline bool hex40_to_20bytes(const std::string& hex, std::uint8_t out[20]) {
        auto hexval = [](char c)->int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
            if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
            return -1;
            };
        if (hex.size() != 40) return false;
        for (int i = 0; i < 20; i++) {
            int hi = hexval(hex[2 * i]);
            int lo = hexval(hex[2 * i + 1]);
            if (hi < 0 || lo < 0) return false;
            out[i] = std::uint8_t((hi << 4) | lo);
        }
        return true;
    }

    // ---------- R1 main dataset ----------
    class R1 {
    public:
        // Fixed behavior: 500k with-replacement from first 250k lines.
        R1() { build_from_file(); }

        std::size_t size() const { return N_; }                     // number of 20B items
        const std::uint8_t* data() const { return buf_.data(); }    // contiguous 20B * N_
        const std::vector<std::uint8_t>& buffer() const { return buf_; }

        // 20-byte stream
        Stream20 make_stream() const { return Stream20(buf_.data(), N_); }

    private:
        void build_from_file() {
            std::ifstream in(filepath);
            if (!in) throw std::runtime_error("R1: cannot open file: " + filepath);

            // 1) Load the FIRST POOL_N valid 40-hex lines only
            std::vector<std::array<std::uint8_t, 20>> pool;
            pool.reserve(POOL_N);
            std::string line;
            while (std::getline(in, line)) {
                // trim spaces
                while (!line.empty() && (line.back() == '\r' || line.back() == '\n' || line.back() == ' ' || line.back() == '\t')) line.pop_back();
                std::size_t start = 0;
                while (start < line.size() && (line[start] == ' ' || line[start] == '\t')) ++start;
                if (line.size() - start < 40) continue;
                std::array<std::uint8_t, 20> b{};
                if (hex40_to_20bytes(line.substr(start, 40), b.data())) {
                    pool.push_back(b);
                    if (pool.size() == POOL_N) break;  // stop at first 250k
                }
            }
            if (pool.empty()) throw std::runtime_error("R1: found no valid SHA-1 lines in: " + filepath);
            if (pool.size() < POOL_N) {
                // still ok: we'll sample from however many were found
                // (but warn via exception text if you prefer strictness)
            }

            // 2) Build STREAM_N items WITH replacement from pool
            N_ = STREAM_N;
            buf_.resize(N_ * 20);
            std::mt19937_64 gen(R1_SAMPLE_SEED);
            std::uniform_int_distribution<std::size_t> pick(0, pool.size() - 1);

            for (std::size_t i = 0; i < N_; ++i) {
                const auto& src = pool[pick(gen)];
                std::uint8_t* out = buf_.data() + i * 20;
                std::copy(src.begin(), src.end(), out);
            }
        }

        std::size_t N_ = 0;
        std::vector<std::uint8_t> buf_;
    };

    // ---------- R1 50/50 split ----------
    class R1Split {
    public:
        explicit R1Split(std::uint64_t split_seed = 0xBEEFCAFE12345678ull) {
            R1 base;
            split_into_groups(base.buffer(), split_seed);
        }

        std::size_t sizeA() const { return A_items_; }
        std::size_t sizeB() const { return B_items_; }

        const std::vector<std::uint8_t>& bufferA() const { return A_buf_; }
        const std::vector<std::uint8_t>& bufferB() const { return B_buf_; }

        Stream20 make_streamA() const { return Stream20(A_buf_.data(), A_items_); }
        Stream20 make_streamB() const { return Stream20(B_buf_.data(), B_items_); }

    private:
        void split_into_groups(const std::vector<std::uint8_t>& sampled, std::uint64_t seed) {
            A_buf_.clear(); B_buf_.clear();
            A_buf_.reserve(sampled.size() / 2 + 20);
            B_buf_.reserve(sampled.size() / 2 + 20);

            const std::size_t items = sampled.size() / 20;
            for (std::size_t i = 0; i < items; ++i) {
                const std::uint64_t g = r1_splitmix64(seed + i) & 1ull; // 0 or 1
                const std::uint8_t* src = sampled.data() + i * 20;
                if (g == 0) A_buf_.insert(A_buf_.end(), src, src + 20);
                else        B_buf_.insert(B_buf_.end(), src, src + 20);
            }
            A_items_ = A_buf_.size() / 20;
            B_items_ = B_buf_.size() / 20;
        }

        std::vector<std::uint8_t> A_buf_, B_buf_;
        std::size_t A_items_ = 0, B_items_ = 0;
    };

} // namespace datasets
