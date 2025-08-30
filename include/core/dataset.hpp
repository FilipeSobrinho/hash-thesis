#pragma once
#include <cstddef>
#include <cstdint>

struct Stream {
  virtual ~Stream() = default;
  // Provides a pointer to the key bytes and its length in bytes.
  // Returns false when the stream is exhausted.
  virtual bool next(const void*& out_ptr, std::size_t& out_len) = 0;
  // Rewind to the beginning of the stream.
  virtual void reset() = 0;
};
