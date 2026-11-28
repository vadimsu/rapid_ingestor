#pragma once

#include <vector>
#include <memory>
#include <unordered_map>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_af_helper.hh"
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_stats.hh"
#include "rapid_ingestor_clickhouse_sink.hh"

namespace RapidIngestor{

	class IProtocol;
	class ClickHouseSink;
	class PipelineStage;

	struct PipelineEdge {
		PipelineStage* target = nullptr;
		seastar::sstring port;
	};

	class PipelineStage {
		public:
			virtual ~PipelineStage() = default;
			virtual void process(std::string_view tag,
				const StackLogFrame& frame) = 0;

			void addDownstream(PipelineStage* target, seastar::sstring port = {}) {
				_downstream.push_back({target, std::move(port)});
			}

		protected:
			void emit(std::string_view tag, const StackLogFrame& frame) {
				for (const auto& edge : _downstream) {
					if (!edge.port.empty() &&
						!matchesPort(edge.port, tag)) {
						continue;
					}
					edge.target->process(tag, frame);
				}
			}

		private:
			static bool matchesPort(std::string_view port, std::string_view tag) {
				return (port == "traces" && tag == "otel.trace") ||
					(port == "metrics" && tag == "otel.metrics") ||
					(port == "logs" && tag == "otel.log");
			}

			std::vector<PipelineEdge> _downstream;
	};

	class PipelineForwardStage final : public PipelineStage {
		public:
			void process(std::string_view tag,
				const StackLogFrame& frame) override {
				emit(tag, frame);
			}
	};

	class PipelineSinkStage final : public PipelineStage {
		public:
			explicit PipelineSinkStage(
				seastar::lw_shared_ptr<ClickHouseSink> sink)
				: _sink(std::move(sink)) {}

			void process(std::string_view tag,
				const StackLogFrame& frame) override;

			ClickHouseSink& sink() const { return *_sink; }

		private:
			seastar::lw_shared_ptr<ClickHouseSink> _sink;
	};

	class PipelineRouter {
		public:
			PipelineRouter(std::vector<Sink> sinks, PipelineGraph graph,
				seastar::sstring source_id);

			seastar::future<> start();
			void add(std::string_view tag, const StackLogFrame& frame);
			bool isOverloaded() const;
			seastar::future<> waitForCapacity();
			uint64_t rowsInserted() const;

		private:
			std::vector<seastar::lw_shared_ptr<ClickHouseSink>> _sinks;
			std::unordered_map<seastar::sstring, size_t> _sink_by_id;
			std::unordered_map<seastar::sstring, size_t> _component_by_id;
			std::unordered_map<seastar::sstring, PipelineStage*> _stages;
			std::vector<std::unique_ptr<PipelineStage>> _stage_storage;
			std::vector<PipelineComponent> _components;
			seastar::sstring _source_id;
			PipelineStage* _source_stage = nullptr;
	};
	typedef enum {
		FORWARD_PROTOCOL
	}ProtocolType;

	class Listener : public seastar::enable_lw_shared_from_this<Listener> {
		public:
			Listener(): _protocol_type(FORWARD_PROTOCOL){
			}
			~Listener(){fmt::print("{} {}\n",__func__,__LINE__);}
			// `sink` is optional (database_type empty = no sink configured); when
			// present, a per-shard ClickHouseSink is started before accepting any
			// connections and wired into every accepted Protocol.
			seastar::future<> listen(seastar::sstring ip, uint16_t port,
				std::vector<Sink> sinks, PipelineGraph graph,
				seastar::sstring source_id, seastar::sstring protocol_type);
			seastar::future<> onProtocolDone(seastar::shared_ptr<IProtocol> protocol);
			RapidIngestorStats getStats();
		private:
			seastar::shared_ptr<AfHelper> _afHelper;
			std::unordered_map<seastar::socket_address, seastar::shared_ptr<IProtocol>> _protocols;
			RapidIngestorStats _stats;
			seastar::lw_shared_ptr<PipelineRouter> _router;
			ProtocolType _protocol_type;
	};
}
