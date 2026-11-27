
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_unix_af_helper.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_otel.hh"
#include "rapid_ingestor_forward_protocol.hh"
#include "rapid_ingestor_clickhouse_sink.hh"
#include <algorithm>
#include <unordered_set>

namespace RapidIngestor{

PipelineRouter::PipelineRouter(std::vector<Sink> sinks, PipelineGraph graph,
	seastar::sstring source_id)
	: _components(graph.components()), _source_id(std::move(source_id)) {
	for (size_t i = 0; i < _components.size(); ++i) {
		_component_by_id.emplace(_components[i].id, i);
		if (_components[i].type != "clickhouse" &&
			_components[i].type != "otel" &&
			_components[i].type != "opentelemetry" &&
			_components[i].type != "forward" &&
			_components[i].type != "TCP" &&
			_components[i].type != "TLS" &&
			_components[i].type != "UNIX") {
			throw std::invalid_argument(fmt::format(
				"runtime pipeline component '{}' of type '{}' is not implemented; "
				"transforms are compile-only",
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
			_edges[upstream].push_back({
				_component_by_id.at(component.id),
				has_port ? seastar::sstring(input.data() + separator + 1,
					input.size() - separator - 1) : seastar::sstring{}});
		}
	}

	if (_component_by_id.find(_source_id) == _component_by_id.end()) {
		throw std::invalid_argument(fmt::format(
			"runtime source '{}' is not present in the pipeline graph", _source_id));
	}

	std::unordered_set<seastar::sstring> reachable;
	std::function<void(const seastar::sstring&)> mark_reachable =
		[&](const seastar::sstring& component_id) {
			if (!reachable.insert(component_id).second) {
				return;
			}
			auto edge_it = _edges.find(component_id);
			if (edge_it == _edges.end()) {
				return;
			}
			for (const auto& edge : edge_it->second) {
				mark_reachable(_components[edge.target].id);
			}
		};
	mark_reachable(_source_id);

	for (const auto& sink_config : sinks) {
		if (!reachable.contains(sink_config.id)) {
			continue;
		}
		auto sink = seastar::make_lw_shared<ClickHouseSink>(sink_config);
		_sink_by_id.emplace(sink_config.id, _sinks.size());
		_sinks.push_back(std::move(sink));
	}
}

seastar::future<> PipelineRouter::start() {
	for (auto& sink : _sinks) {
		co_await sink->start();
	}
	co_return;
}

void PipelineRouter::visit(size_t component, std::string_view tag,
	const StackLogFrame& frame, std::vector<bool>& visited) {
	if (visited[component]) {
		return;
	}
	visited[component] = true;
	const auto& current = _components[component];
	if (current.type == "clickhouse") {
		auto sink_it = _sink_by_id.find(current.id);
		if (sink_it == _sink_by_id.end()) {
			throw std::logic_error(fmt::format(
				"pipeline sink '{}' has no runtime sink", current.id));
		}
		_sinks[sink_it->second]->add(tag, frame);
		return;
	}
	auto edge_it = _edges.find(current.id);
	if (edge_it == _edges.end()) {
		return;
	}
	for (const auto& edge : edge_it->second) {
		if (!edge.port.empty()) {
			const bool matches =
				(edge.port == "traces" && tag == "otel.trace") ||
				(edge.port == "metrics" && tag == "otel.metrics") ||
				(edge.port == "logs" && tag == "otel.log");
			if (!matches) {
				continue;
			}
		}
		visit(edge.target, tag, frame, visited);
	}
}

void PipelineRouter::add(std::string_view tag, const StackLogFrame& frame) {
	auto source_it = _component_by_id.find(_source_id);
	if (source_it == _component_by_id.end()) {
		return;
	}
	std::vector<bool> visited(_components.size(), false);
	visit(source_it->second, tag, frame, visited);
}

bool PipelineRouter::isOverloaded() const {
	return std::any_of(_sinks.begin(), _sinks.end(),
		[](const auto& sink) { return sink->isOverloaded(); });
}

seastar::future<> PipelineRouter::waitForCapacity() {
	for (auto& sink : _sinks) {
		if (sink->isOverloaded()) {
			co_await sink->waitForCapacity();
		}
	}
}

uint64_t PipelineRouter::rowsInserted() const {
	uint64_t rows = 0;
	for (const auto& sink : _sinks) {
		rows += sink->getRowsInserted();
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
