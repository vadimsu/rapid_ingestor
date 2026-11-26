
#include <unordered_map>
#include <vector>
#include <nghttp2/nghttp2.h>
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
	if(_session){
		nghttp2_session_del(_session);
		_session = nullptr;
	}
}

seastar::future<RapidIngestorStats> OtelProtocol::onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener){
	_connection = connection;
	_listener = listener;
	auto holder = _gate.hold(); // Held until both loops finish

	init_session();
	maybe_wake_writer();

        // Run read and write fibers concurrently on this shard
	try{
       		co_await seastar::when_all_succeed(read_loop(), write_loop());
	}catch(...){
	}
	co_return seastar::make_ready_future<RapidIngestorStats>(getStats());
}

const seastar::socket_address& OtelProtocol::getAddress(){ return _addr; }
seastar::future<> OtelProtocol::stop(){
	if (!_closed) {
		_closed = true;
		co_await _gate.close();
	}
	int last_stream_id = 0;
	nghttp2_submit_goaway(_session, NGHTTP2_FLAG_NONE, last_stream_id, NGHTTP2_NO_ERROR, nullptr, 0);
	_write_cv.signal();

	co_await _connection->stop();
	co_return;
}

RapidIngestorStats& OtelProtocol::getStats() {
	return _stats;
}

void OtelProtocol::setSink(ProtocolEngine::RecordSink sink) {}

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

void send_grpc_error(int32_t stream_id, GrpcStatus grpcStatus){
}

void handle_trace_batch(int32_t stream_id, opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest&& req){
}

void handle_metrics_batch(int32_t stream_id, opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest&& req){
}

void  handle_logs_batch(int32_t stream_id, opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest&& req){
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
			// Hand `std::move(req)` to your ingestion pipeline / shard / ClickHouse writer
			handle_trace_batch(stream->stream_id, std::move(req));
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
			handle_metrics_batch(stream->stream_id, std::move(req));
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
			handle_logs_batch(stream->stream_id, std::move(req));
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
	nghttp2_session_callbacks_new(&cbs);

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
        nghttp2_session_server_new(&_session, cbs, this);
        // Queue mandatory server connection preface (SETTINGS frame)
        nghttp2_settings_entry iv[] = {
            { NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 256 },
            { NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 1024 * 1024 } // 1 MiB per stream
        };
        nghttp2_submit_settings(_session, NGHTTP2_FLAG_NONE, iv, 2);
}

seastar::future<> OtelProtocol::read_loop() {
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

		// mem_recv2 may have auto-queued ACKs/WINDOW_UPDATEs/GOAWAY
		maybe_wake_writer();

		if (rv < 0) {
			break; // HTTP/2 protocol error
		}
		if (_isOverloaded && _isOverloaded()) {
			co_await _waitForCapacity();
		}
	}
	_closed = true;
        _write_cv.signal(); // Wake writer so it can drain GOAWAY and exit
	co_return;
}


seastar::future<> OtelProtocol::write_loop() {
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
				if (len < 0) _closed = true;
				break;
			}
			co_await _connection->send(reinterpret_cast<const char*>(data), static_cast<size_t>(len));
		}

//		co_await out_.flush();

		// Exit once closed and nothing left to send (e.g., after GOAWAY flushes)
		if (_closed && !nghttp2_session_want_write(_session)) {
			break;
		}
	}
	co_await _connection->stop();
	co_return;
}


// Call this after ANY nghttp2_submit_*() or mem_recv2()
void OtelProtocol::maybe_wake_writer() {
	if (nghttp2_session_want_write(_session)) {
		_write_cv.signal();
	}
}

}
