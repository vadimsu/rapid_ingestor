
#pragma once

#include <algorithm>
#include <limits>
#include <google/protobuf/io/zero_copy_stream.h>

class ChunkedZeroCopyInputStream final : public google::protobuf::io::ZeroCopyInputStream {
    const std::vector<seastar::temporary_buffer<char>>& chunks_;
    size_t idx_;
    size_t off_;
    size_t remaining_;
    int64_t byte_count_ = 0;

public:
    ChunkedZeroCopyInputStream(
        const std::vector<seastar::temporary_buffer<char>>& chunks,
        const GrpcFrameHeader& hdr)
        : chunks_(chunks),
          idx_(hdr.payload_chunk_idx),
          off_(hdr.payload_offset),
          remaining_(hdr.length) {}

    bool Next(const void** data, int* size) override {
        while (remaining_ > 0 && idx_ < chunks_.size()) {
            if (off_ >= chunks_[idx_].size()) {
                ++idx_;
                off_ = 0;
                continue;
            }
            size_t avail = std::min(chunks_[idx_].size() - off_, remaining_);
            avail = std::min<size_t>(avail, std::numeric_limits<int>::max());

            *data = chunks_[idx_].get() + off_;
            *size = static_cast<int>(avail);

            off_ += avail;
            remaining_ -= avail;
            byte_count_ += static_cast<int64_t>(avail);
            return true;
        }
        return false;
    }

    void BackUp(int count) override {
        // Protobuf guarantees BackUp() is only called after Next() with count <= last size
        off_ -= static_cast<size_t>(count);
        remaining_ += static_cast<size_t>(count);
        byte_count_ -= count;
    }

    bool Skip(int count) override {
        size_t to_skip = static_cast<size_t>(count);
        while (to_skip > 0 && remaining_ > 0 && idx_ < chunks_.size()) {
            if (off_ >= chunks_[idx_].size()) {
                ++idx_;
                off_ = 0;
                continue;
            }
            size_t step = std::min({chunks_[idx_].size() - off_, remaining_, to_skip});
            off_ += step;
            remaining_ -= step;
            to_skip -= step;
            byte_count_ += static_cast<int64_t>(step);
        }
        return to_skip == 0;
    }

    int64_t ByteCount() const override { return byte_count_; }
};
