
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_unix_af_helper.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_otel.hh"
#include "rapid_ingestor_forward_protocol.hh"
#include "rapid_ingestor_clickhouse_sink.hh"
#include <algorithm>
#include <unordered_set>

namespace RapidIngestor{

void PipelineComponentRegistry::registerType(
	seastar::sstring type, PipelineProcessFn process) {
	if (!process || type.empty()) {
		throw std::invalid_argument("pipeline component registration is invalid");
	}
	if (!_factories.emplace(std::move(type), process).second) {
		throw std::invalid_argument("duplicate pipeline component registration");
	}
}

void PipelineComponentRegistry::registerFactory(
	seastar::sstring type, FactoryFn factory) {
	if (!factory || type.empty()) {
		throw std::invalid_argument("pipeline component factory is invalid");
	}
	if (!_component_factories.emplace(std::move(type), factory).second) {
		throw std::invalid_argument("duplicate pipeline component factory");
	}
}

PipelineProcessFn PipelineComponentRegistry::processFor(
	std::string_view type) const {
	auto it = _factories.find(seastar::sstring(type));
	return it == _factories.end() ? nullptr : it->second;
}

bool PipelineComponentRegistry::hasType(std::string_view type) const {
	auto key = seastar::sstring(type);
	return _factories.find(key) != _factories.end() ||
		_component_factories.find(key) != _component_factories.end();
}

PipelineComponentRegistry::Instance PipelineComponentRegistry::create(
	const PipelineComponent& component, const Sink* config) const {
	auto factory = _component_factories.find(component.type);
	if (factory != _component_factories.end()) {
		return factory->second(component, config);
	}
	auto process = processFor(component.type);
	if (!process) {
		throw std::invalid_argument(fmt::format(
			"runtime component type '{}' is not registered", component.type));
	}
	return {process, nullptr, {}, nullptr, nullptr, nullptr, nullptr};
}

void PipelinePortRegistry::registerPort(
	seastar::sstring name, seastar::sstring tag) {
	if (name.empty() || tag.empty() || !_tags.emplace(
			std::move(name), std::move(tag)).second) {
		throw std::invalid_argument("duplicate or invalid pipeline port registration");
	}
}

std::optional<seastar::sstring> PipelinePortRegistry::tagFor(
	std::string_view name) const {
	auto it = _tags.find(seastar::sstring(name));
	if (it == _tags.end()) {
		return std::nullopt;
	}
	return it->second;
}

namespace {
void forward_pipeline_record(PipelineExecutionPlan& plan, size_t node,
	void*, std::string_view tag, const StackLogFrame& frame) {
	plan.emit(node, tag, frame);
}

void filter_pipeline_record(PipelineExecutionPlan& plan, size_t node,
	void* raw_state, std::string_view tag, const StackLogFrame& frame) {
	auto* state = static_cast<const PipelineStringFilter*>(raw_state);
	if (state->matches(frame)) {
		plan.emit(node, tag, frame);
	}
}

void sink_pipeline_record(PipelineExecutionPlan&,
	size_t, void* state, std::string_view tag, const StackLogFrame& frame) {
	static_cast<ClickHouseSink*>(state)->add(tag, frame);
}

seastar::future<> clickhouse_start(void* state) {
	return static_cast<ClickHouseSink*>(state)->start();
}
bool clickhouse_overloaded(const void* state) {
	return static_cast<const ClickHouseSink*>(state)->isOverloaded();
}
seastar::future<> clickhouse_wait(void* state) {
	return static_cast<ClickHouseSink*>(state)->waitForCapacity();
}
uint64_t clickhouse_rows(const void* state) {
	return static_cast<const ClickHouseSink*>(state)->getRowsInserted();
}

struct ClickHouseOwner {
	seastar::lw_shared_ptr<ClickHouseSink> sink;
};

PipelineComponentRegistry::Instance clickhouse_factory(
	const PipelineComponent&, const Sink* config) {
	if (!config) {
		throw std::invalid_argument("ClickHouse factory requires sink configuration");
	}
	auto* owner = new ClickHouseOwner{
		seastar::make_lw_shared<ClickHouseSink>(*config)};
	std::shared_ptr<void> lifetime(owner, [](void* value) {
		delete static_cast<ClickHouseOwner*>(value);
	});
	return {&sink_pipeline_record, owner->sink.get(), std::move(lifetime),
		&clickhouse_start, &clickhouse_overloaded, &clickhouse_wait,
		&clickhouse_rows};
}

PipelineComponentRegistry::Instance filter_factory(
	const PipelineComponent& component, const Sink*) {
	if (component.filter_field.empty()) {
		throw std::invalid_argument(fmt::format(
			"filter '{}' has no field predicate", component.id));
	}
	auto* state = new PipelineStringFilter{
		std::string(component.filter_field),
		std::string(component.filter_equals)};
	std::shared_ptr<void> lifetime(state, [](void* value) {
		delete static_cast<PipelineStringFilter*>(value);
	});
	return {&filter_pipeline_record, state, std::move(lifetime),
		nullptr, nullptr, nullptr, nullptr};
}
}

PipelineRouter::PipelineRouter(std::vector<Sink> sinks, PipelineGraph graph,
	seastar::sstring source_id, const PipelineComponentRegistry& components,
	const PipelinePortRegistry& ports)
	: _components(graph.components()), _source_id(std::move(source_id)) {
	for (size_t i = 0; i < _components.size(); ++i) {
		_component_by_id.emplace(_components[i].id, i);
		if (!components.hasType(_components[i].type)) {
			throw std::invalid_argument(fmt::format(
				"runtime pipeline component '{}' of type '{}' is not registered",
				_components[i].id, _components[i].type));
		}
	}

	for (const auto& component : _components) {
		for (const auto& input : component.inputs) {
			auto separator = input.find('.');
			const bool has_port = separator != seastar::sstring::npos;
			auto upstream = has_port
				? seastar::sstring(input.data(), separator) : input;
			auto upstream_it = _component_by_id.find(upstream);
			if (upstream_it == _component_by_id.end()) {
				continue;
			}
			// Edges are connected after stage construction below.
		}
	}

	if (_component_by_id.find(_source_id) == _component_by_id.end()) {
		throw std::invalid_argument(fmt::format(
			"runtime source '{}' is not present in the pipeline graph", _source_id));
	}

	std::function<bool(const seastar::sstring&,
		std::unordered_set<seastar::sstring>&)> is_reachable =
		[&](const seastar::sstring& component_id,
			std::unordered_set<seastar::sstring>& visiting) {
			if (component_id == _source_id) {
				return true;
			}
			if (!visiting.insert(component_id).second) {
				return false;
			}
			auto component_it = _component_by_id.find(component_id);
			if (component_it == _component_by_id.end()) {
				return false;
			}
			for (const auto& input : _components[component_it->second].inputs) {
				auto separator = input.find('.');
				auto upstream = separator == seastar::sstring::npos
					? input : seastar::sstring(input.data(), separator);
				if (is_reachable(upstream, visiting)) {
					return true;
				}
			}
			return false;
		};

	std::unordered_set<seastar::sstring> reachable_sinks;
	for (const auto& sink : sinks) {
		std::unordered_set<seastar::sstring> visiting;
		if (is_reachable(sink.id, visiting)) {
			reachable_sinks.insert(sink.id);
		}
	}

	for (const auto& component : _components) {
		const Sink* sink_config = nullptr;
		for (const auto& candidate : sinks) {
			if (candidate.id == component.id) {
				sink_config = &candidate;
				break;
			}
		}
		if (sink_config && !reachable_sinks.contains(component.id)) {
			continue;
		}
		auto instance = components.create(component, sink_config);
		if (!instance.process) {
			throw std::invalid_argument(fmt::format(
				"runtime component '{}' factory returned no process function",
				component.id));
		}
		auto index = _instances.size();
		_instances.push_back(std::move(instance));
		_sink_by_id.emplace(component.id, index);
	}

	for (const auto& component : _components) {
		auto instance_it = _sink_by_id.find(component.id);
		if (instance_it == _sink_by_id.end()) {
			continue;
		}
		auto& instance = _instances[instance_it->second];
		auto node = _plan.addNode(instance.process, instance.state);
		_stage_by_id.emplace(component.id, node);
	}

	for (const auto& component : _components) {
		auto target_it = _stage_by_id.find(component.id);
		if (target_it == _stage_by_id.end()) {
			continue;
		}
		for (const auto& input : component.inputs) {
			auto separator = input.find('.');
			const bool has_port = separator != seastar::sstring::npos;
			auto upstream = has_port
				? seastar::sstring(input.data(), separator) : input;
			auto upstream_it = _stage_by_id.find(upstream);
			if (upstream_it == _stage_by_id.end()) {
				continue;
			}
			auto port = has_port
				? seastar::sstring(input.data() + separator + 1,
					input.size() - separator - 1)
				: seastar::sstring{};
			if (!port.empty()) {
				auto tag = ports.tagFor(port);
				if (!tag) {
					throw std::invalid_argument(fmt::format(
						"pipeline port '{}' has no runtime tag mapping", port));
				}
				_plan.connect(upstream_it->second, target_it->second, port);
				_plan.indexTaggedEdge(upstream_it->second, *tag);
			} else {
				_plan.connect(upstream_it->second, target_it->second);
			}
		}
	}
	_source_stage = _stage_by_id.at(_source_id);
	_has_source_stage = true;
}

const PipelineComponentRegistry& PipelineRouter::defaultComponents() {
	static const auto registry = [] {
		PipelineComponentRegistry value;
		value.registerFactory("clickhouse", &clickhouse_factory);
		value.registerFactory("filter", &filter_factory);
		value.registerType("otel", &forward_pipeline_record);
		value.registerType("opentelemetry", &forward_pipeline_record);
		value.registerType("forward", &forward_pipeline_record);
		value.registerType("TCP", &forward_pipeline_record);
		value.registerType("TLS", &forward_pipeline_record);
		value.registerType("UNIX", &forward_pipeline_record);
		return value;
	}();
	return registry;
}

const PipelinePortRegistry& PipelineRouter::defaultPorts() {
	static const auto registry = [] {
		PipelinePortRegistry value;
		value.registerPort("traces", "otel.trace");
		value.registerPort("metrics", "otel.metrics");
		value.registerPort("logs", "otel.log");
		return value;
	}();
	return registry;
}

seastar::future<> PipelineRouter::start() {
	for (auto& instance : _instances) {
		if (instance.start) {
			co_await instance.start(instance.state);
		}
	}
	co_return;
}

void PipelineRouter::add(std::string_view tag, const StackLogFrame& frame) {
	if (_has_source_stage) {
		_plan.dispatch(_source_stage, tag, frame);
	}
}

bool PipelineRouter::isOverloaded() const {
	return std::any_of(_instances.begin(), _instances.end(),
		[](const auto& instance) {
			return instance.overloaded && instance.overloaded(instance.state);
		});
}

seastar::future<> PipelineRouter::waitForCapacity() {
	for (auto& instance : _instances) {
		if (instance.overloaded && instance.overloaded(instance.state)) {
			co_await instance.waitForCapacity(instance.state);
		}
	}
}

uint64_t PipelineRouter::rowsInserted() const {
	uint64_t rows = 0;
	for (const auto& instance : _instances) {
		if (instance.rowsInserted) {
			rows += instance.rowsInserted(instance.state);
		}
	}
	return rows;
}

seastar::future<> Listener::listen(seastar::sstring ip, uint16_t port,
	std::vector<Sink> sinks, PipelineGraph graph,
	seastar::sstring source_id, seastar::sstring protocol_type){
	try {
		_router = seastar::make_lw_shared<PipelineRouter>(
			std::move(sinks), std::move(graph), std::move(source_id));
	} catch (...) {
		fmt::print(stderr, "pipeline router construction failed for {}:{} ({})\n",
			ip, port, protocol_type);
		throw;
	}
	try {
		co_await _router->start();
	} catch (...) {
		fmt::print(stderr, "pipeline sink startup failed for {}:{} ({})\n",
			ip, port, protocol_type);
		throw;
	}
	if (port == 0){//unix
		fmt::print("removing unix domain socket file {}\n",ip);
		seastar::remove_file(fmt::format("{}", ip));
		_afHelper = seastar::make_shared<RapidIngestor::UnixAfHelper>(ip, port);
	}else{
		_afHelper = seastar::make_shared<RapidIngestor::TcpAfHelper>(ip, port);
	}
	co_await seastar::do_until([this]{
		return false;
	},
	[this, protocol_type]{
		return _afHelper->accept().then([this, protocol_type] (auto ar) mutable {
			seastar::connected_socket fd = std::move(ar.connection);
			seastar::socket_address addr = std::move(ar.remote_address);
			seastar::shared_ptr<IProtocol> protocol;
			if (protocol_type == "forward"){
				protocol = seastar::make_shared<ForwardProtocol>(addr);
			}else if  (protocol_type == "otel"){
				protocol = seastar::make_shared<OtelProtocol>(addr);
			}
			fmt::print("Protocol type {}\n",protocol_type);
			auto conn = seastar::make_lw_shared<Connection>(std::move(fd), addr, protocol_type == "otel");
//			fmt::print("accepted {}\n",addr);
			if (_router) {
				auto router = _router;
				protocol->setSink([router] (std::string_view tag, const StackLogFrame& frame) {
					router->add(tag, frame);
				});
				protocol->setBackpressure(
					[router] { return router->isOverloaded(); },
					[router] { return router->waitForCapacity(); });
			}
			_protocols.emplace(protocol->getAddress(), protocol);
			protocol->onAccepted(conn, this).then([this] (RapidIngestorStats stats){
				_stats += stats;
				return seastar::make_ready_future<>();
			});
		});
	});
}


seastar::future<> Listener::onProtocolDone(seastar::shared_ptr<IProtocol> protocol){
	return protocol->stop().then([this, protocol] {
		_protocols.erase(protocol->getAddress());
		return seastar::make_ready_future<>();
	});
}

RapidIngestorStats Listener::getStats(){
	RapidIngestorStats current_stats;
	for (auto& p : _protocols){
		current_stats += p.second->getStats();
	}
	current_stats.rowsInserted = _router ? _router->rowsInserted() : 0;
	return _stats + current_stats;
}

}
