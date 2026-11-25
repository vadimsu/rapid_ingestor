#pragma once

#include <unordered_map>
#include <vector>
#include <nghttp2/nghttp2.h>
#include "rapid_ingestor_otel.hh"
#include "rapid_ingestor_nghttp2_callbacks.hh"

namespace RapidIngestor{

// 1. Allocate/bind StreamCtx when a new request HEADERS frame begins
int on_begin_headers(nghttp2_session* session, const nghttp2_frame* frame,
                     void* user_data) {
    if (frame->hd.type != NGHTTP2_HEADERS ||
        frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
        return 0;
    }

    auto* conn = static_cast<OtelProtocol*>(user_data);
    auto* stream = conn->create_stream(frame->hd.stream_id); // e.g., from pool or map
    nghttp2_session_set_stream_user_data(session, frame->hd.stream_id, stream);
    return 0;
}

// 2. Capture :path and grpc-encoding as HPACK decodes each header
int on_header(nghttp2_session* session, const nghttp2_frame* frame,
              const uint8_t* name, size_t namelen,
              const uint8_t* value, size_t valuelen,
              uint8_t /*flags*/, void* /*user_data*/) {
    if (frame->hd.type != NGHTTP2_HEADERS ||
        frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
        return 0;
    }

    auto* stream = static_cast<StreamCtx*>(
        nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
    if (!stream) return 0;

    std::string_view k(reinterpret_cast<const char*>(name), namelen);
    std::string_view v(reinterpret_cast<const char*>(value), valuelen);

    fmt::print("{} {} {} {}\n",__FILE__,__LINE__,k,v);
    if (k == ":path") {
        if (v == "/opentelemetry.proto.collector.trace.v1.TraceService/Export") {
            stream->signal = OtlpSignal::Traces;
        } else if (v == "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export") {
            stream->signal = OtlpSignal::Metrics;
        } else if (v == "/opentelemetry.proto.collector.logs.v1.LogsService/Export") {
            stream->signal = OtlpSignal::Logs;
        }
    } else if (k == "grpc-encoding" && v == "gzip") {
        stream->gzip_compressed = true;
    }
    return 0;
}

// 3. Append incoming DATA frame chunks into the stream's receive buffer
int on_data_chunk_recv(nghttp2_session* session, uint8_t /*flags*/,
                       int32_t stream_id, const uint8_t* data, size_t len,
                       void* user_data) {
    auto* otelProtocol = static_cast<OtelProtocol*>(user_data);
    auto* stream = static_cast<StreamCtx*>(
        nghttp2_session_get_stream_user_data(session, stream_id));
    if (!stream || !otelProtocol) return 0;
#if 0

    fmt::print("{} {} {} {}\n",__FILE__,__func__,stream_id,len);
    // Optional: enforce a max payload cap (e.g., 16 MiB) and RST_STREAM if exceeded
    stream->recv_buf.append(reinterpret_cast<const char*>(data), len);
    stream->total_bytes += len;
#else
    if (len == 0) return 0;
    fmt::print("{} {} {} {}\n",__FILE__,__func__,stream_id,len);
    // Compute offset inside the active Seastar temporary_buffer
    auto* base = reinterpret_cast<const uint8_t*>(otelProtocol->getCurrentBuf().get());
    size_t offset = static_cast<size_t>(data - base);

    // Zero-copy ref-counted slice
    stream->chunks.push_back(otelProtocol->getCurrentBuf().share(offset, len));
    stream->total_bytes += len;
#endif
    return 0;
}

// 4. Detect END_STREAM and dispatch the complete gRPC request
int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data) {
    auto* conn = static_cast<OtelProtocol*>(user_data);

    if ((frame->hd.type == NGHTTP2_DATA || frame->hd.type == NGHTTP2_HEADERS) &&
        (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {

        auto* stream = static_cast<StreamCtx*>(
            nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
        if (!stream) return 0;

        // Dispatch to your OTLP/gRPC handler (sync or detached Seastar coroutine)
        conn->dispatch_otlp_request(stream);
    }
    return 0;
}

// 5. Release StreamCtx when the stream finishes or is reset by the peer
int on_stream_close(nghttp2_session* session, int32_t stream_id,
                    uint32_t /*error_code*/, void* user_data) {
    auto* conn = static_cast<OtelProtocol*>(user_data);
    conn->destroy_stream(stream_id); // Removes from map / returns to pool
    return 0;
}

}
