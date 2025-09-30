#pragma once
// datasets/r2.hpp
// R2: take the first 1e5 *words* from a text file (whitespace-separated).
// - Each word is materialized as UTF-8 bytes in a flat buffer; we store offsets/lengths.
// - Streaming API yields (ptr,len) for each word.
// - R2Split: split those 1e5 words into first half and second half (position-based).
//
// Default source file: "first_1e6_words.txt" (but any UTF-8 text file works).

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>
#include <algorithm>

#ifndef ROOT_DEFAULT_DIR
#define ROOT_DEFAULT_DIR "."
#endif

namespace datasets {

    static constexpr const char* R2_DEFAULT_FILE = "first_1e6_words.txt";
    static constexpr std::size_t R2_TAKE_N = 500'000;

    // -------- variable-length stream over (flat bytes + (offset,len) pairs) --------
    class StreamVar {
    public:
        StreamVar() = default;
        StreamVar(const std::uint8_t* base, const std::vector<std::pair<std::uint32_t, std::uint32_t>>* idx)
            : base_(base), idx_(idx) {
        }

        void reset(const std::uint8_t* base, const std::vector<std::pair<std::uint32_t, std::uint32_t>>* idx) {
            base_ = base; idx_ = idx; i_ = 0;
        }

        bool next(const void*& out_ptr, std::size_t& out_len) {
            if (!idx_ || i_ >= idx_->size()) return false;
            const auto [off, len] = (*idx_)[i_++];
            out_ptr = base_ + off;
            out_len = len;
            return true;
        }

        void rewind() { i_ = 0; }
        std::size_t size_hint() const { return idx_ ? idx_->size() : 0; }

    private:
        const std::uint8_t* base_ = nullptr;
        const std::vector<std::pair<std::uint32_t, std::uint32_t>>* idx_ = nullptr;
        std::size_t i_ = 0;
    };

    // ------------------------------ R2 (first 1e5 words) ------------------------------
    class R2 {
    public:
        explicit R2(const std::string& path = std::string(ROOT_DEFAULT_DIR) + "/" + R2_DEFAULT_FILE) {
            build_from_file(path);
        }

        std::size_t size() const { return idx_.size(); }
        const std::vector<std::uint8_t>& buffer() const { return buf_; }
        const std::vector<std::pair<std::uint32_t, std::uint32_t>>& index() const { return idx_; }

        StreamVar make_stream() const { return StreamVar(buf_.data(), &idx_); }

    private:
        static inline void append_word(const std::string& w,
            std::vector<std::uint8_t>& buf,
            std::vector<std::pair<std::uint32_t, std::uint32_t>>& idx)
        {
            const std::uint32_t off = static_cast<std::uint32_t>(buf.size());
            buf.insert(buf.end(), reinterpret_cast<const std::uint8_t*>(w.data()),
                reinterpret_cast<const std::uint8_t*>(w.data()) + w.size());
            const std::uint32_t len = static_cast<std::uint32_t>(w.size());
            idx.emplace_back(off, len);
        }

        void build_from_file(const std::string& path) {
            std::ifstream in(path);
            if (!in) throw std::runtime_error("R2: cannot open file: " + path);

            buf_.clear(); idx_.clear(); buf_.reserve(4'000'000); idx_.reserve(R2_TAKE_N);

            // Stream tokens without loading the whole file.
            std::string token;
            while (in >> token) {
                append_word(token, buf_, idx_);
                if (idx_.size() == R2_TAKE_N) break;
            }
            if (idx_.empty())
                throw std::runtime_error("R2: no tokens parsed from: " + path);
            // It’s fine if file had < 1e5 words; we keep what we got.
        }

        std::vector<std::uint8_t> buf_;
        std::vector<std::pair<std::uint32_t, std::uint32_t>> idx_;
    };

    // ------------------------------ R2Split (first half vs second half) ------------------------------
    class R2Split {
    public:
        explicit R2Split(const std::string& path = std::string(ROOT_DEFAULT_DIR) + "/" + R2_DEFAULT_FILE) {
            R2 base(path);
            split_halves(base.buffer(), base.index());
        }

        std::size_t sizeA() const { return idxA_.size(); }
        std::size_t sizeB() const { return idxB_.size(); }

        const std::vector<std::uint8_t>& bufferA() const { return bufA_; }
        const std::vector<std::uint8_t>& bufferB() const { return bufB_; }

        StreamVar make_streamA() const { return StreamVar(bufA_.data(), &idxA_); }
        StreamVar make_streamB() const { return StreamVar(bufB_.data(), &idxB_); }

    private:
        static inline void copy_word(const std::uint8_t* src_base, std::uint32_t off, std::uint32_t len,
            std::vector<std::uint8_t>& out_buf,
            std::vector<std::pair<std::uint32_t, std::uint32_t>>& out_idx)
        {
            const std::uint32_t new_off = static_cast<std::uint32_t>(out_buf.size());
            out_buf.insert(out_buf.end(), src_base + off, src_base + off + len);
            out_idx.emplace_back(new_off, len);
        }

        void split_halves(const std::vector<std::uint8_t>& base_buf,
            const std::vector<std::pair<std::uint32_t, std::uint32_t>>& base_idx)
        {
            const std::size_t N = base_idx.size();
            const std::size_t mid = N / 2;

            // Pre-size roughly to avoid lots of reallocs (heuristic)
            bufA_.reserve(base_buf.size() / 2 + 1024);
            bufB_.reserve(base_buf.size() / 2 + 1024);
            idxA_.reserve(mid);
            idxB_.reserve(N - mid);

            for (std::size_t i = 0; i < N; ++i) {
                const auto [off, len] = base_idx[i];
                if (i < mid)
                    copy_word(base_buf.data(), off, len, bufA_, idxA_);
                else
                    copy_word(base_buf.data(), off, len, bufB_, idxB_);
            }
        }

        std::vector<std::uint8_t> bufA_, bufB_;
        std::vector<std::pair<std::uint32_t, std::uint32_t>> idxA_, idxB_;
    };

} // namespace datasets
