#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <string_view>
#include <seastar/core/seastar.hh>
#include <seastar/core/condition-variable.hh>
#include <nghttp2/nghttp2.h>
#include "rapid_ingestor_listener.hh"
#include "rapid_ingestor_protocol.hh"
#include "rapid_ingestor_grpc.hh"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.pb.h"

namespace RapidIngestor{

struct StreamCtx;

class OtelProtocol : public IProtocol {
	public:
		OtelProtocol(seastar::socket_address addr);
		~OtelProtocol();
		seastar::future<RapidIngestorStats> onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener);
		const seastar::socket_address& getAddress();
		seastar::future<> stop();
		RapidIngestorStats& getStats();
		void setSink(ProtocolEngine::RecordSink sink);
		using IsOverloaded = std::function<bool()>;
		using WaitForCapacity = std::function<seastar::future<>()>;
		void setBackpressure(IsOverloaded isOverloaded, WaitForCapacity waitForCapacity);
		StreamCtx* create_stream(int32_t stream_id);
		void dispatch_otlp_request(StreamCtx*);
		void destroy_stream(int32_t stream_id);
		seastar::temporary_buffer<char> getCurrentBuf() { return _current_buf.share(); }
	private:
		nghttp2_session_callbacks* init_h2_callbacks();

		void init_session();

		seastar::future<> read_loop();

		seastar::future<> write_loop();

		// Call this after ANY nghttp2_submit_*() or mem_recv2()
		void maybe_wake_writer();
		void send_grpc_ok(int32_t stream_id);
		void send_grpc_error(int32_t stream_id, GrpcStatus grpc_status);
		void handle_trace_batch(
			const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest& request);
		void handle_metrics_batch(
			const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest& request);
		void handle_logs_batch(
			const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request);
		void emit_json_record(std::string_view tag, const google::protobuf::Message& message);
		seastar::socket_address _addr;
		seastar::lw_shared_ptr<Connection> _connection;
		nghttp2_session* _session;
		Listener* _listener;
		RapidIngestorStats _stats;
		bool _fatal_error = false;
		IsOverloaded _isOverloaded;
		WaitForCapacity _waitForCapacity;
		seastar::condition_variable _write_cv;
		bool _closed = false;
		std::unordered_map<int32_t, StreamCtx*> _streams;
		seastar::temporary_buffer<char> _current_buf;
		// Temporary direct sink binding; OTLP pipeline stages will replace this.
		ProtocolEngine::RecordSink _sink;
		std::string _json_buffer;
};

}
