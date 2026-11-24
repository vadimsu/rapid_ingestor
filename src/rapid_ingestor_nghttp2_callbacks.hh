#pragma once

#include <unordered_map>
#include <vector>
#include <nghttp2/nghttp2.h>

namespace RapidIngestor{

struct StreamCtx; 

class OtelProtocol;

int on_begin_headers(nghttp2_session* session, const nghttp2_frame* frame,
                     void* user_data);

int on_header(nghttp2_session* session, const nghttp2_frame* frame,
              const uint8_t* name, size_t namelen,
              const uint8_t* value, size_t valuelen,
              uint8_t /*flags*/, void* /*user_data*/);

int on_data_chunk_recv(nghttp2_session* session, uint8_t /*flags*/,
                       int32_t stream_id, const uint8_t* data, size_t len,
                       void* /*user_data*/);

int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data);

int on_stream_close(nghttp2_session* session, int32_t stream_id,
                    uint32_t /*error_code*/, void* user_data);

#if 0
int on_data_chunk_recv(nghttp2_session* session,
                       uint8_t flags,
                       int32_t stream_id,
                       const uint8_t* data,
                       size_t len,
                       void* user_data) {
    auto* ctx = static_cast<ConnectionContext*>(user_data);
    auto it = ctx->streams.find(stream_id);
    if (it == ctx->streams.end() || len == 0) {
        return 0;
    }

    // Calculate offset within the active Seastar temporary_buffer
    const char* chunk_ptr = reinterpret_cast<const char*>(data);
    size_t offset = static_cast<size_t>(chunk_ptr - ctx->current_buf->get());

    // Create a zero-copy, reference-counted slice
    it->second.chunks.push_back(ctx->current_buf->share(offset, len));
    it->second.total_bytes += len;

    return 0;
}

#endif

enum class OtlpSignal : uint8_t { Unknown, Traces, Metrics, Logs };

struct StreamCtx {
    StreamCtx(int32_t stream_id):_stream_id(stream_id){}
    std::string path;
    int32_t _stream_id = 0;
    OtlpSignal signal = OtlpSignal::Unknown;
    bool gzip_compressed = false;
    std::vector<seastar::temporary_buffer<char>> chunks;
    size_t total_bytes = 0;
    std::string recv_buf; // Accumulates 5-byte gRPC prefix + Protobuf payload
    std::string send_buf; // Holds 5-byte gRPC prefix + serialized response
    size_t send_offset = 0;
};

struct ConnectionContext {
    // Points to the active buffer during nghttp2_session_mem_recv()
    const seastar::temporary_buffer<char>* current_buf = nullptr;
    std::unordered_map<int32_t, StreamCtx> streams;
};

class OtelProtocol;

// 1. Allocate/bind StreamCtx when a new request HEADERS frame begins
int on_begin_headers(nghttp2_session* session, const nghttp2_frame* frame,
                     void* user_data);

// 2. Capture :path and grpc-encoding as HPACK decodes each header
int on_header(nghttp2_session* session, const nghttp2_frame* frame,
              const uint8_t* name, size_t namelen,
              const uint8_t* value, size_t valuelen,
              uint8_t /*flags*/, void* /*user_data*/);

// 3. Append incoming DATA frame chunks into the stream's receive buffer
int on_data_chunk_recv(nghttp2_session* session, uint8_t /*flags*/,
                       int32_t stream_id, const uint8_t* data, size_t len,
                       void* /*user_data*/);

// 4. Detect END_STREAM and dispatch the complete gRPC request
int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data);

// 5. Release StreamCtx when the stream finishes or is reset by the peer
int on_stream_close(nghttp2_session* session, int32_t stream_id,
                    uint32_t /*error_code*/, void* user_data);

}
