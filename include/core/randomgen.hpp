/*
 * This file is based on https://github.com/kipoujr/no_repetition/blob/main/src/framework/randomgen.h
 *
 */


#pragma once
#ifndef RNG_DEFAULT_SEED_DIR
// Define the default seed directory if not defined by build system
#define RNG_DEFAULT_SEED_DIR "./seed"
#endif
#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <mutex>
#include <stdexcept>
#include <fstream>
#include <algorithm>
#include <filesystem>



namespace rng {

    class RandomPool {
    public:
        static RandomPool& instance() {
            static RandomPool inst;
            return inst;
        }

        void init() {
            std::scoped_lock lk(mu_);
            if (initialized_) return;

            const std::filesystem::path dir = std::filesystem::path(RNG_DEFAULT_SEED_DIR);
            if (!std::filesystem::exists(dir)) {
                throw std::runtime_error("RandomPool: seed dir not found: " + dir.string());
            }

            std::vector<std::filesystem::path> files;
            for (const auto& e : std::filesystem::directory_iterator(dir)) {
                if (e.is_regular_file() && e.path().extension() == ".bin") files.push_back(e.path());
            }
            std::sort(files.begin(), files.end());
            if (files.empty()) {
                throw std::runtime_error("RandomPool: no .bin files in " + dir.string());
            }

            std::vector<std::uint8_t> tmp;
            for (const auto& p : files) {
                std::ifstream in(p, std::ios::binary);
                if (!in) throw std::runtime_error("RandomPool: cannot open " + p.string());
                in.seekg(0, std::ios::end);
                const std::streamsize sz = in.tellg();
                in.seekg(0, std::ios::beg);
                if (sz <= 0) continue;
                const std::size_t off = tmp.size();
                tmp.resize(off + static_cast<std::size_t>(sz));
                in.read(reinterpret_cast<char*>(tmp.data() + off), sz);
            }
            if (tmp.empty()) throw std::runtime_error("RandomPool: total bytes read = 0");

            bytes_.swap(tmp);
            pos_ = 0;
            bit_bucket_ = 0;
            bit_pos_ = 8;
            initialized_ = true;
        }

        // Draw APIs (auto-init on first use)
        std::uint8_t  u8() { ensure(); std::scoped_lock lk(mu_); auto v = bytes_[pos_]; advance(1); return v; }
        bool          boolean() {
            ensure(); std::scoped_lock lk(mu_);
            if (bit_pos_ == 8) { bit_bucket_ = bytes_[pos_]; advance(1); bit_pos_ = 0; }
            const bool b = (bit_bucket_ & 0x01u) != 0u;
            bit_bucket_ >>= 1; ++bit_pos_;
            return b;
        }
        std::uint32_t u32() { std::uint32_t a = 0; a = (a << 8) | u8(); a = (a << 8) | u8(); a = (a << 8) | u8(); a = (a << 8) | u8(); return a; }
        std::uint64_t u64() { std::uint64_t a = 0; for (int i = 0; i < 8; ++i) a = (a << 8) | u8(); return a; }


    private:
        RandomPool() = default;

        void ensure() { if (!initialized_) init(); }
        void advance(std::size_t n) { pos_ += n; if (pos_ >= bytes_.size()) pos_ %= bytes_.size(); }

        mutable std::mutex mu_;
        bool initialized_{ false };
        std::vector<std::uint8_t> bytes_;
        std::size_t pos_{ 0 };
        std::uint8_t bit_bucket_{ 0 };
        std::uint8_t bit_pos_{ 8 };
    };

    // Convenience wrappers
    inline std::uint8_t  get_u8() { return RandomPool::instance().u8(); }
    inline bool          get_bool() { return RandomPool::instance().boolean(); }
    inline std::uint32_t get_u32() { return RandomPool::instance().u32(); }
    inline std::uint64_t get_u64() { return RandomPool::instance().u64(); }


} // namespace rng
