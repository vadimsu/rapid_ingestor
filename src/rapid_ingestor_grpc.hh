
#pragma once

#include <cstdint>
#include <optional>
#include <vector>
#include <seastar/core/temporary_buffer.hh>

struct GrpcFrameHeader {
    bool compressed = false;
    uint32_t length = 0;
    size_t payload_chunk_idx = 0;
    size_t payload_offset = 0;
};

enum class GrpcStatus : int {
    Ok = 0,
    InvalidArgument = 3,
    Unimplemented = 12,
    Internal = 13,
};

struct GrpcHeaderResult {
    GrpcStatus status = GrpcStatus::Ok;
    GrpcFrameHeader header;
};

static inline GrpcHeaderResult parse_grpc_header(
    const std::vector<seastar::temporary_buffer<char>>& chunks,
    size_t total_bytes) {

    if (total_bytes < 5) {
        return {GrpcStatus::Internal, {}}; // Truncated gRPC prefix
    }

    uint8_t hdr[5];
    size_t copied = 0;
    size_t idx = 0;
    size_t off = 0;

    while (copied < 5 && idx < chunks.size()) {
        size_t avail = chunks[idx].size() - off;
        size_t take = std::min(5 - copied, avail);
        std::memcpy(hdr + copied, chunks[idx].get() + off, take);
        copied += take;
        off += take;
        if (off == chunks[idx].size()) {
            ++idx;
            off = 0;
        }
    }

    if (hdr[0] > 1) {
        return {GrpcStatus::Internal, {}}; // Invalid compression flag
    }

    uint32_t msg_len = (uint32_t(hdr[1]) << 24) |
                       (uint32_t(hdr[2]) << 16) |
                       (uint32_t(hdr[3]) << 8)  |
                        uint32_t(hdr[4]);

    if (total_bytes - 5 < msg_len) {
        return {GrpcStatus::Internal, {}}; // Truncated payload
    }

    return {
        GrpcStatus::Ok,
        GrpcFrameHeader{
            .compressed = (hdr[0] == 1),
            .length = msg_len,
            .payload_chunk_idx = idx,
            .payload_offset = off,
        }
    };
}
