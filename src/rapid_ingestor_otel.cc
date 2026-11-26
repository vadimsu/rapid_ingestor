
#include <unordered_map>
#include <vector>
#include <array>
#include <cstring>
#include <stdexcept>
#include <nghttp2/nghttp2.h>
#include <google/protobuf/util/json_util.h>
#include "rapid_ingestor_listener.hh"
#include "rapid_ingestor_otel.hh"
#include "rapid_ingestor_nghttp2_callbacks.hh"
#include "rapid_ingestor_otlp.hh"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.pb.h"

namespace RapidIngestor{

extern "C" {
	nghttp2_ssize nghttp2_session_mem_send2(nghttp2_session *session,
                                        const uint8_t **data_ptr);
};

OtelProtocol::OtelProtocol(seastar::socket_address addr) : _addr(addr), _session(nullptr){}
OtelProtocol::~OtelProtocol(){
	for (auto& [id, stream] : _streams) {
		delete stream;
	}
	if(_session){
		nghttp2_session_del(_session);
		_session = nullptr;
	}
}

seastar::future<RapidIngestorStats> OtelProtocol::onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener){
	_connection = connection;
	_listener = listener;
	try{
		init_session();
		maybe_wake_writer();
       		co_await seastar::when_all_succeed(read_loop(), write_loop());
	}catch(const std::exception& e){
		fmt::print(stderr, "OTLP connection {} failed: {}\n", _addr, e.what());
		++_stats.fatalErrors;
	}
	auto stats = getStats();
	co_await _listener->onProtocolDone(this->shared_from_this());
	co_return stats;
}

const seastar::socket_address& OtelProtocol::getAddress(){ return _addr; }
seastar::future<> OtelProtocol::stop(){
	_closed = true;
	_write_cv.signal();
	co_await _connection->stop();
	co_return;
}

RapidIngestorStats& OtelProtocol::getStats() {
	return _stats;
}

void OtelProtocol::setSink(ProtocolEngine::RecordSink sink) {
	_sink = std::move(sink);
}

void OtelProtocol::setBackpressure(IsOverloaded isOverloaded, WaitForCapacity waitForCapacity) {
	_isOverloaded = std::move(isOverloaded);
	_waitForCapacity = std::move(waitForCapacity);
}

StreamCtx* OtelProtocol::create_stream(int32_t stream_id){
	StreamCtx* stream = nullptr;
	auto it = _streams.find(stream_id);
	if (it == _streams.end()){
		stream = new StreamCtx(stream_id);
		_streams.emplace(stream_id, stream);
	}else{
		stream = it->second;
	}
	return stream;
}

void OtelProtocol::send_grpc_error(int32_t stream_id, GrpcStatus grpc_status) {
	const char* status = nullptr;
	const char* message = nullptr;
	switch (grpc_status) {
		case GrpcStatus::InvalidArgument:
			status = "3";
			message = "invalid argument";
			break;
		case GrpcStatus::Unimplemented:
			status = "12";
			message = "unimplemented";
			break;
		case GrpcStatus::Internal:
			status = "13";
			message = "internal";
			break;
		case GrpcStatus::Ok:
			return;
		default:
			status = "13";
			message = "internal";
			break;
	}

	auto make_header = [](const char* name, const char* value) {
		return nghttp2_nv{
			reinterpret_cast<uint8_t*>(const_cast<char*>(name)),
			reinterpret_cast<uint8_t*>(const_cast<char*>(value)),
			std::strlen(name),
			std::strlen(value),
			NGHTTP2_NV_FLAG_NONE};
	};

	std::array<nghttp2_nv, 4> headers{
		make_header(":status", "200"),
		make_header("content-type", "application/grpc"),
		make_header("grpc-status", status),
		make_header("grpc-message", message)};

	int rv = nghttp2_submit_response(
		_session, stream_id, headers.data(), headers.size(), nullptr);
	if (rv != 0) {
		fmt::print(stderr, "Failed to submit gRPC error response on stream {}: {}\n",
		           stream_id, nghttp2_strerror(rv));
		return;
	}
	maybe_wake_writer();
}

void OtelProtocol::send_grpc_ok(int32_t stream_id) {
	auto make_header = [](const char* name, const char* value) {
		return nghttp2_nv{
			reinterpret_cast<uint8_t*>(const_cast<char*>(name)),
			reinterpret_cast<uint8_t*>(const_cast<char*>(value)),
			std::strlen(name),
			std::strlen(value),
			NGHTTP2_NV_FLAG_NONE};
	};

	std::array<nghttp2_nv, 4> headers{
		make_header(":status", "200"),
		make_header("content-type", "application/grpc"),
		make_header("grpc-status", "0"),
		make_header("grpc-message", "")};

	int rv = nghttp2_submit_response(
		_session, stream_id, headers.data(), headers.size(), nullptr);
	if (rv != 0) {
		fmt::print(stderr, "Failed to submit gRPC success response on stream {}: {}\n",
		           stream_id, nghttp2_strerror(rv));
		return;
	}
	maybe_wake_writer();
}

void OtelProtocol::emit_json_record(
	std::string_view tag, const google::protobuf::Message& message) {
	_json_buffer.clear();
	auto status = google::protobuf::util::MessageToJsonString(message, &_json_buffer);
	if (!status.ok()) {
		fmt::print(stderr, "Failed to serialize OTLP record as JSON: {}\n",
		           status.message());
		++_stats.fatalErrors;
		return;
	}

	if (!_sink) {
		return;
	}

	StackLogFrame frame;
	frame.count = 1;
	frame.elements[0] = MsgPackElement{
		.type = MsgPackType::String,
		.key = "payload",
		.val = _json_buffer};
	_sink(tag, frame);
}

void OtelProtocol::handle_trace_batch(
	const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest& request) {
	for (const auto& resource_spans : request.resource_spans()) {
		++_stats.messagesParsed;
		_stats.bytesProcessed += resource_spans.ByteSizeLong();
		emit_json_record("otel.trace", resource_spans);
	}
}

void OtelProtocol::handle_metrics_batch(
	const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest& request) {
	for (const auto& resource_metrics : request.resource_metrics()) {
		++_stats.messagesParsed;
		_stats.bytesProcessed += resource_metrics.ByteSizeLong();
		emit_json_record("otel.metrics", resource_metrics);
	}
}

void OtelProtocol::handle_logs_batch(
	const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request) {
	for (const auto& resource_logs : request.resource_logs()) {
		++_stats.messagesParsed;
		_stats.bytesProcessed += resource_logs.ByteSizeLong();
		emit_json_record("otel.logs", resource_logs);
	}
}

void OtelProtocol::dispatch_otlp_request(StreamCtx* stream){
	if (!stream) return;

	switch (stream->signal) {
		case OtlpSignal::Traces: {
			opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest req;
			GrpcStatus st = decode_grpc_message(
						stream->recv_chunks,
						stream->total_recv_bytes,
						stream->gzip_compressed,
						req);

			// Release socket buffer slices early—they are no longer needed after parsing
			stream->recv_chunks.clear();
			stream->total_recv_bytes = 0;

			if (st != GrpcStatus::Ok) {
				send_grpc_error(stream->stream_id, st);
				return;
			}
			// Hand the decoded request to the ingestion pipeline / shard / ClickHouse writer.
			handle_trace_batch(req);
			send_grpc_ok(stream->stream_id);
			break;
		}

		case OtlpSignal::Metrics: {
			opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest req;
			GrpcStatus st = decode_grpc_message(
						stream->recv_chunks,
						stream->total_recv_bytes,
						stream->gzip_compressed,
						req);

			stream->recv_chunks.clear();
			stream->total_recv_bytes = 0;

			if (st != GrpcStatus::Ok) {
				send_grpc_error(stream->stream_id, st);
				return;
			}
			handle_metrics_batch(req);
			send_grpc_ok(stream->stream_id);
			break;
		}

		case OtlpSignal::Logs: {
			opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest req;
			GrpcStatus st = decode_grpc_message(
						stream->recv_chunks,
						stream->total_recv_bytes,
						stream->gzip_compressed,
						req);

			stream->recv_chunks.clear();
			stream->total_recv_bytes = 0;

			if (st != GrpcStatus::Ok) {
				send_grpc_error(stream->stream_id, st);
				return;
			}
			handle_logs_batch(req);
			send_grpc_ok(stream->stream_id);
			break;
		}

		case OtlpSignal::Unknown:
		default:
			stream->recv_chunks.clear();
			stream->total_recv_bytes = 0;
			send_grpc_error(stream->stream_id, GrpcStatus::Unimplemented);
			break;
	}
}

void OtelProtocol::destroy_stream(int32_t stream_id){
	auto it = _streams.find(stream_id);
	if (it != _streams.end()){
		delete it->second;
		_streams.erase(it);
	}
}

nghttp2_session_callbacks* OtelProtocol::init_h2_callbacks() {
	nghttp2_session_callbacks* cbs = nullptr;
	int rv = nghttp2_session_callbacks_new(&cbs);
	if (rv != 0) throw std::runtime_error(nghttp2_strerror(rv));

	nghttp2_session_callbacks_set_on_begin_headers_callback(cbs, on_begin_headers);
	nghttp2_session_callbacks_set_on_header_callback(cbs, on_header);
	nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, on_data_chunk_recv);
	nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, on_frame_recv);
	nghttp2_session_callbacks_set_on_stream_close_callback(cbs, on_stream_close);

	// Note: No send_callback needed because we will use pull-based mem_send2()
	return cbs;
}

void OtelProtocol::init_session(){
	nghttp2_session_callbacks* cbs = init_h2_callbacks();
	// Bind `this` as user_data for all session callbacks
	int rv = nghttp2_session_server_new(&_session, cbs, this);
	nghttp2_session_callbacks_del(cbs);
	if (rv != 0) throw std::runtime_error(nghttp2_strerror(rv));
        // Queue mandatory server connection preface (SETTINGS frame)
        nghttp2_settings_entry iv[] = {
            { NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 256 },
            { NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 1024 * 1024 } // 1 MiB per stream
        };
	rv = nghttp2_submit_settings(_session, NGHTTP2_FLAG_NONE, iv, 2);
	if (rv != 0) throw std::runtime_error(nghttp2_strerror(rv));
}

seastar::future<> OtelProtocol::read_loop() {
	try {
	while (!_closed && nghttp2_session_want_read(_session)) {
		// Yields to the Seastar reactor until bytes arrive
		auto buf = co_await _connection->receive();
		if (buf.empty()) {
			break; // Peer closed TCP connection (EOF)
		}
		_current_buf = buf.share();	
		nghttp2_ssize rv = nghttp2_session_mem_recv2(
			_session,
			reinterpret_cast<const uint8_t*>(buf.get()),
			buf.size()
		);
		_current_buf = {};

		// mem_recv2 may have auto-queued ACKs/WINDOW_UPDATEs/GOAWAY
		maybe_wake_writer();

		if (rv < 0) {
			throw std::runtime_error(nghttp2_strerror(rv));
		}
		if (_isOverloaded && _isOverloaded()) {
			co_await _waitForCapacity();
		}
	}
	} catch (...) {
		_closed = true;
		_write_cv.signal();
		_connection->shutdown();
		throw;
	}
	_closed = true;
        _write_cv.signal(); // Wake writer so it can drain GOAWAY and exit
	co_return;
}


seastar::future<> OtelProtocol::write_loop() {
	try {
	for (;;) {
		// Wait until nghttp2 actually has bytes to send (or connection closes)
		co_await _write_cv.wait([this] {
			return _closed || nghttp2_session_want_write(_session);
		});

		// Drain all currently queued HTTP/2 frames
		while (nghttp2_session_want_write(_session)) {
			const uint8_t* data = nullptr;
			nghttp2_ssize len = nghttp2_session_mem_send2(_session, &data);
			if (len <= 0) {
				if (len < 0) throw std::runtime_error(nghttp2_strerror(len));
				break;
			}
			co_await _connection->send(reinterpret_cast<const char*>(data), static_cast<size_t>(len));
		}

//		co_await out_.flush();

		// Exit once closed and nothing left to send (e.g., after GOAWAY flushes)
		if (_closed) {
			break;
		}
	}
	} catch (...) {
		_closed = true;
		_connection->shutdown();
		throw;
	}
	co_return;
}


// Call this after ANY nghttp2_submit_*() or mem_recv2()
void OtelProtocol::maybe_wake_writer() {
	if (nghttp2_session_want_write(_session)) {
		_write_cv.signal();
	}
}

}
