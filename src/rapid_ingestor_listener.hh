#pragma once

#include <vector>
#include <memory>
#include <unordered_map>
#include <optional>
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
	class PipelineExecutionPlan;
	using PipelineProcessFn = void (*)(PipelineExecutionPlan&, size_t, void*,
		std::string_view, const StackLogFrame&);

	struct PipelineStringFilter {
		std::string field;
		std::string expected;
		int64_t expected_integer = 0;
		bool expected_boolean = false;
		PipelineFilterValueKind kind = PipelineFilterValueKind::String;

		bool matches(const StackLogFrame& frame) const {
			auto value = frame.find(field);
			if (!value) {
				return false;
			}
			switch (kind) {
				case PipelineFilterValueKind::String:
					return value->type == MsgPackType::String &&
						value->val == expected;
				case PipelineFilterValueKind::Integer:
					return value->type == MsgPackType::Integer &&
						value->as_int() == expected_integer;
				case PipelineFilterValueKind::Boolean:
					return value->type == MsgPackType::Boolean &&
						!value->val.empty() &&
						(value->val[0] != 0) == expected_boolean;
			}
			return false;
		}
	};

	class PipelineComponentRegistry {
		public:
			struct Instance {
				PipelineProcessFn process = nullptr;
				void* state = nullptr;
				std::shared_ptr<void> owner;
				using StartFn = seastar::future<> (*)(void*);
				using OverloadedFn = bool (*)(const void*);
				using WaitFn = seastar::future<> (*)(void*);
				using RowsFn = uint64_t (*)(const void*);
				StartFn start = nullptr;
				OverloadedFn overloaded = nullptr;
				WaitFn waitForCapacity = nullptr;
				RowsFn rowsInserted = nullptr;
			};
			using FactoryFn = Instance (*)(const PipelineComponent&, const Sink*);

			void registerType(seastar::sstring type, PipelineProcessFn process);
			void registerFactory(seastar::sstring type, FactoryFn factory);
			PipelineProcessFn processFor(std::string_view type) const;
			bool hasType(std::string_view type) const;
			Instance create(const PipelineComponent& component,
				const Sink* config) const;

		private:
			std::unordered_map<seastar::sstring, PipelineProcessFn> _factories;
			std::unordered_map<seastar::sstring, FactoryFn> _component_factories;
	};

	class PipelinePortRegistry {
		public:
			void registerPort(seastar::sstring name, seastar::sstring tag);
			std::optional<seastar::sstring> tagFor(std::string_view name) const;

		private:
			std::unordered_map<seastar::sstring, seastar::sstring> _tags;
	};

	struct PipelineStringHash {
		using is_transparent = void;
		size_t operator()(std::string_view value) const noexcept {
			return std::hash<std::string_view>{}(value);
		}
		size_t operator()(const std::string& value) const noexcept {
			return operator()(std::string_view(value));
		}
		size_t operator()(const seastar::sstring& value) const noexcept {
			return operator()(std::string_view(value));
		}
	};

	struct PipelineStringEqual {
		using is_transparent = void;
		bool operator()(std::string_view left, std::string_view right) const noexcept {
			return left == right;
		}
	};

	struct CompiledPipelineEdge {
		size_t target = 0;
		seastar::sstring port;
	};

	class PipelineExecutionPlan {
		public:
			using ProcessFn = PipelineProcessFn;

			struct Node {
				ProcessFn process = nullptr;
				void* state = nullptr;
				std::vector<CompiledPipelineEdge> outputs;
				std::unordered_map<std::string, std::vector<size_t>,
					PipelineStringHash, PipelineStringEqual>
					outputs_by_tag;
				std::vector<size_t> untagged_outputs;
			};

			size_t addNode(ProcessFn process, void* state) {
				_nodes.push_back({process, state, {}, {}, {}});
				return _nodes.size() - 1;
			}

			void connect(size_t source, size_t target, seastar::sstring port = {}) {
				auto& node = _nodes[source];
				const auto edge_index = node.outputs.size();
				node.outputs.push_back({target, std::move(port)});
				if (node.outputs.back().port.empty()) {
					node.untagged_outputs.push_back(edge_index);
				}
			}

			void dispatch(size_t node, std::string_view tag,
				const StackLogFrame& frame) {
				auto& current = _nodes[node];
				current.process(*this, node, current.state, tag, frame);
			}

			void emit(size_t source, std::string_view tag,
				const StackLogFrame& frame) {
				auto& node = _nodes[source];
				for (auto edge_index : node.untagged_outputs) {
					dispatch(node.outputs[edge_index].target, tag, frame);
				}
				auto tagged = node.outputs_by_tag.find(tag);
				if (tagged != node.outputs_by_tag.end()) {
					for (auto edge_index : tagged->second) {
						dispatch(node.outputs[edge_index].target, tag, frame);
					}
				}
			}

			void indexTaggedEdge(size_t source, std::string tag) {
				auto& node = _nodes[source];
				node.outputs_by_tag[std::move(tag)].push_back(
					node.outputs.size() - 1);
			}

		private:
			std::vector<Node> _nodes;
	};

	class PipelineRouter {
		public:
			PipelineRouter(std::vector<Sink> sinks, PipelineGraph graph,
				seastar::sstring source_id,
				const PipelineComponentRegistry& components = defaultComponents(),
				const PipelinePortRegistry& ports = defaultPorts());

			seastar::future<> start();
			void add(std::string_view tag, const StackLogFrame& frame);
			bool isOverloaded() const;
			seastar::future<> waitForCapacity();
			uint64_t rowsInserted() const;

		private:
			std::vector<PipelineComponentRegistry::Instance> _instances;
			std::unordered_map<seastar::sstring, size_t> _sink_by_id;
			std::unordered_map<seastar::sstring, size_t> _component_by_id;
			PipelineExecutionPlan _plan;
			std::unordered_map<seastar::sstring, size_t> _stage_by_id;
			std::vector<PipelineComponent> _components;
			seastar::sstring _source_id;
			size_t _source_stage = 0;
			bool _has_source_stage = false;

			static const PipelineComponentRegistry& defaultComponents();
			static const PipelinePortRegistry& defaultPorts();
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
