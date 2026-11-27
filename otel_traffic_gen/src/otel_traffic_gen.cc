#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>

#include <nghttp2/nghttp2.h>
#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>
#include <opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <stdexcept>
#include <vector>

namespace bpo = boost::program_options;

namespace {

struct Stats {
    uint64_t batches = 0;
    uint64_t bytes = 0;

    Stats& operator+=(const Stats& other) {
        batches += other.batches;
        bytes += other.bytes;
        return *this;
    }
};

enum class Signal { Traces, Metrics, Logs };

struct Payload {
    std::string wire;
    size_t offset = 0;
};

struct ClientState {
    int32_t stream_id = -1;
    bool stream_closed = false;
    uint32_t stream_error = 0;
    bool grpc_status_received = false;
    bool grpc_ok = false;
};

Signal parse_signal(const seastar::sstring& signal) {
    if (signal == "traces") return Signal::Traces;
    if (signal == "metrics") return Signal::Metrics;
    if (signal == "logs") return Signal::Logs;
    throw std::invalid_argument("signal must be traces, metrics, or logs");
}

seastar::socket_address make_target_address(
    const seastar::sstring& host, uint16_t port, const seastar::sstring& unix_path) {
    if (!unix_path.empty()) {
        return seastar::socket_address(seastar::unix_domain_addr{unix_path});
    }
    return seastar::make_ipv4_address({seastar::net::inet_address(host), port});
}

std::string path_for(Signal signal) {
    switch (signal) {
        case Signal::Traces:
            return "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
        case Signal::Metrics:
            return "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
        case Signal::Logs:
            return "/opentelemetry.proto.collector.logs.v1.LogsService/Export";
    }
    return {};
}

std::string make_request_payload(Signal signal) {
    std::string payload;
    switch (signal) {
        case Signal::Traces: {
            opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest request;
            auto* resource_spans = request.add_resource_spans();
            auto* scope_spans = resource_spans->add_scope_spans();
            auto* span = scope_spans->add_spans();
            span->set_trace_id("rapid-ingestor-trace");
            span->set_span_id("rapid-ingestor-span");
            span->set_name("otel-traffic-generator");
            span->set_start_time_unix_nano(1);
            span->set_end_time_unix_nano(2);
            if (!request.SerializeToString(&payload)) {
                throw std::runtime_error("failed to serialize trace payload");
            }
            break;
        }
        case Signal::Metrics: {
            opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest request;
            auto* resource_metrics = request.add_resource_metrics();
            auto* scope_metrics = resource_metrics->add_scope_metrics();
            auto* metric = scope_metrics->add_metrics();
            metric->set_name("rapid_ingestor.traffic");
            metric->set_description("OTLP traffic generator sample");
            metric->mutable_gauge()->add_data_points()->set_as_double(1.0);
            if (!request.SerializeToString(&payload)) {
                throw std::runtime_error("failed to serialize metrics payload");
            }
            break;
        }
        case Signal::Logs: {
            opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
            auto* resource_logs = request.add_resource_logs();
            auto* scope_logs = resource_logs->add_scope_logs();
            auto* log = scope_logs->add_log_records();
            log->set_time_unix_nano(1);
            log->mutable_body()->set_string_value("rapid-ingestor OTLP traffic generator");
            if (!request.SerializeToString(&payload)) {
                throw std::runtime_error("failed to serialize logs payload");
            }
            break;
        }
    }

    if (payload.size() > UINT32_MAX) {
        throw std::length_error("OTLP payload exceeds the gRPC message limit");
    }

    std::string wire;
    wire.resize(5);
    wire[0] = 0;
    wire[1] = static_cast<char>((payload.size() >> 24) & 0xff);
    wire[2] = static_cast<char>((payload.size() >> 16) & 0xff);
    wire[3] = static_cast<char>((payload.size() >> 8) & 0xff);
    wire[4] = static_cast<char>(payload.size() & 0xff);
    wire += payload;
    return wire;
}

nghttp2_nv make_header(const char* name, const char* value) {
    return {
        reinterpret_cast<uint8_t*>(const_cast<char*>(name)),
        reinterpret_cast<uint8_t*>(const_cast<char*>(value)),
        std::strlen(name),
        std::strlen(value),
        NGHTTP2_NV_FLAG_NONE};
}

nghttp2_ssize read_payload(
    nghttp2_session*, int32_t, uint8_t* buffer, size_t length,
    uint32_t* data_flags, nghttp2_data_source* source, void*) {
    auto* payload = static_cast<Payload*>(source->ptr);
    size_t remaining = payload->wire.size() - payload->offset;
    size_t count = std::min(length, remaining);
    std::memcpy(buffer, payload->wire.data() + payload->offset, count);
    payload->offset += count;
    if (payload->offset == payload->wire.size()) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    return static_cast<nghttp2_ssize>(count);
}

int on_stream_close(
    nghttp2_session*, int32_t stream_id, uint32_t error_code, void* user_data) {
    auto* state = static_cast<ClientState*>(user_data);
    if (stream_id == state->stream_id) {
        state->stream_closed = true;
        state->stream_error = error_code;
    }
    return 0;
}

int on_header(
    nghttp2_session*, const nghttp2_frame* frame,
    const uint8_t* name, size_t namelen, const uint8_t* value, size_t valuelen,
    uint8_t, void* user_data) {
    auto* state = static_cast<ClientState*>(user_data);
    if (frame->hd.stream_id == state->stream_id &&
        std::string_view(reinterpret_cast<const char*>(name), namelen) == "grpc-status") {
        state->grpc_status_received = true;
        state->grpc_ok = std::string_view(reinterpret_cast<const char*>(value), valuelen) == "0";
    }
    return 0;
}

class Worker {
public:
    Stats get_stats() const { return _stats; }

    seastar::future<> run(
        seastar::socket_address target, Signal signal, unsigned connections,
        uint64_t batches, uint32_t rate) {
        std::vector<seastar::future<>> futures;
        futures.reserve(connections);
        for (unsigned i = 0; i < connections; ++i) {
            futures.push_back(run_connection(target, signal, batches, rate));
        }
        co_await seastar::when_all_succeed(std::move(futures));
    }

private:
    seastar::future<> run_connection(
        seastar::socket_address target, Signal signal,
        uint64_t batches, uint32_t rate) {
        auto fd = co_await seastar::connect(target);
        auto input = fd.input();
        auto output = fd.output();
        nghttp2_session* session = nullptr;
        nghttp2_session_callbacks* callbacks = nullptr;
        ClientState client_state;
        int rv = nghttp2_session_callbacks_new(&callbacks);
        if (rv != 0) throw std::runtime_error(nghttp2_strerror(rv));
        std::unique_ptr<nghttp2_session_callbacks, decltype(&nghttp2_session_callbacks_del)>
            callbacks_guard(callbacks, nghttp2_session_callbacks_del);
        nghttp2_session_callbacks_set_on_stream_close_callback(
            callbacks, on_stream_close);
        nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header);
        rv = nghttp2_session_client_new(&session, callbacks, &client_state);
        if (rv != 0) throw std::runtime_error(nghttp2_strerror(rv));
        std::unique_ptr<nghttp2_session, decltype(&nghttp2_session_del)>
            session_guard(session, nghttp2_session_del);
        nghttp2_settings_entry settings[] = {
            {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 128}};
        nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, settings, 1);

        auto payload = Payload{make_request_payload(signal)};
        auto path = path_for(signal);
        auto headers = std::vector<nghttp2_nv>{
            make_header(":method", "POST"),
            make_header(":scheme", "http"),
            make_header(":path", path.c_str()),
            make_header(":authority", "otel-traffic-gen"),
            make_header("content-type", "application/grpc"),
            make_header("te", "trailers")};
        nghttp2_data_provider2 provider{
            .source = {.ptr = &payload},
            .read_callback = read_payload};

        auto interval = rate == 0
            ? std::chrono::microseconds(0)
            : std::chrono::microseconds(1000000 / rate);
        auto next_tick = std::chrono::steady_clock::now();

        std::exception_ptr failure;
        try {
            for (uint64_t sent = 0; batches == 0 || sent < batches; ++sent) {
                payload.offset = 0;
                client_state = {};
                int32_t stream_id = nghttp2_submit_request2(
                    session, nullptr, headers.data(), headers.size(), &provider, nullptr);
                if (stream_id < 0) {
                    throw std::runtime_error(nghttp2_strerror(stream_id));
                }
                client_state.stream_id = stream_id;

                while (nghttp2_session_want_write(session)) {
                    const uint8_t* data = nullptr;
                    nghttp2_ssize length = nghttp2_session_mem_send2(session, &data);
                    if (length < 0) throw std::runtime_error(nghttp2_strerror(length));
                    if (length == 0) break;
                    co_await output.write(reinterpret_cast<const char*>(data), length);
                    _stats.bytes += static_cast<uint64_t>(length);
                }
                co_await output.flush();
                while (!client_state.stream_closed) {
                    auto response = co_await input.read();
                    if (response.empty()) {
                        throw std::runtime_error("server closed before gRPC response");
                    }
                    nghttp2_ssize consumed = nghttp2_session_mem_recv2(
                        session,
                        reinterpret_cast<const uint8_t*>(response.get()),
                        response.size());
                    if (consumed < 0) {
                        throw std::runtime_error(nghttp2_strerror(consumed));
                    }
                }
                if (client_state.stream_error != NGHTTP2_NO_ERROR ||
                    !client_state.grpc_status_received || !client_state.grpc_ok) {
                    throw std::runtime_error("server returned an unsuccessful gRPC response");
                }
                ++_stats.batches;

                if (rate > 0) {
                    next_tick += interval;
                    auto now = std::chrono::steady_clock::now();
                    if (next_tick > now) co_await seastar::sleep(next_tick - now);
                }
            }
        } catch (...) {
            failure = std::current_exception();
        }

        try {
            co_await seastar::when_all_succeed(input.close(), output.close());
        } catch (const std::exception& e) {
            if (!failure) throw;
            fmt::print(stderr, "Failed to close OTLP connection: {}\n", e.what());
        } catch (...) {
            if (!failure) throw;
            fmt::print(stderr, "Failed to close OTLP connection: unknown error\n");
        }
        if (failure) std::rethrow_exception(failure);
    }

    Stats _stats;
};

} // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    app.add_options()
        ("host", bpo::value<seastar::sstring>()->default_value("127.0.0.1"), "target host")
        ("port", bpo::value<uint16_t>()->default_value(4317), "target port")
        ("unix-path", bpo::value<seastar::sstring>()->default_value({}), "Unix socket path")
        ("signal", bpo::value<seastar::sstring>()->default_value("traces"),
            "OTLP signal: traces, metrics, or logs")
        ("connections", bpo::value<unsigned>()->default_value(1), "connections per core")
        ("batches", bpo::value<uint64_t>()->default_value(0), "batches per connection, 0 = unlimited")
        ("rate", bpo::value<uint32_t>()->default_value(0), "batches/sec/connection, 0 = unlimited");

    return app.run(argc, argv, [&app] {
        auto& config = app.configuration();
        auto target = make_target_address(
            config["host"].as<seastar::sstring>(),
            config["port"].as<uint16_t>(),
            config["unix-path"].as<seastar::sstring>());
        auto signal = parse_signal(config["signal"].as<seastar::sstring>());
        auto connections = config["connections"].as<unsigned>();
        auto batches = config["batches"].as<uint64_t>();
        auto rate = config["rate"].as<uint32_t>();

        auto workers = seastar::make_lw_shared<seastar::sharded<Worker>>();
        return workers->start().then([workers, target, signal, connections, batches, rate] {
            return workers->invoke_on_all(
                [target, signal, connections, batches, rate](Worker& worker) {
                    return worker.run(target, signal, connections, batches, rate);
                }).finally([workers] {
                    return workers->stop().finally([workers] {});
                });
        });
    });
}
