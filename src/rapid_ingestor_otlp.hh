
#pragma once

#include "opentelemetry/proto/collector/trace/v1/trace_service.pb.h"
#include "rapid_ingestor_grpc.hh"
#include "rapid_ingestor_protobuf.hh"
// (and metrics_service.pb.h / logs_service.pb.h)

template <typename OtlpRequest>
GrpcStatus decode_grpc_message(
    const std::vector<seastar::temporary_buffer<char>>& chunks,
    size_t total_bytes,
    bool header_says_gzip,
    OtlpRequest& out_msg) {

    auto [status, hdr] = parse_grpc_header(chunks, total_bytes);
    if (status != GrpcStatus::Ok) {
        return status;
    }

    ChunkedZeroCopyInputStream zcis(chunks, hdr);

    if (hdr.compressed) {
        if (!header_says_gzip) {
            return GrpcStatus::Unimplemented; // Unsupported grpc-encoding
        }
        // Hook: wrap `zcis` in google::protobuf::io::GzipInputStream (or zlib)
        return GrpcStatus::Unimplemented;
    }

    if (!out_msg.ParseFromZeroCopyStream(&zcis)) {
        return GrpcStatus::InvalidArgument;
    }
    return GrpcStatus::Ok;
}
