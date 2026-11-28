#include <iostream>
#include <memory>

#include "src/rapid_ingestor_listener.hh"

namespace {

struct MemoryState {
	size_t received = 0;
};

struct FilterState {
	RapidIngestor::PipelineStringFilter predicate;
};

void memory_process(RapidIngestor::PipelineExecutionPlan&, size_t, void* raw,
	std::string_view, const RapidIngestor::StackLogFrame&) {
	++static_cast<MemoryState*>(raw)->received;
}

void source_process(RapidIngestor::PipelineExecutionPlan& plan, size_t node,
	void*, std::string_view tag, const RapidIngestor::StackLogFrame& frame) {
	plan.emit(node, tag, frame);
}

void filter_process(RapidIngestor::PipelineExecutionPlan& plan, size_t node,
	void* raw, std::string_view tag, const RapidIngestor::StackLogFrame& frame) {
	if (static_cast<const FilterState*>(raw)->predicate.matches(frame)) {
		plan.emit(node, tag, frame);
	}
}

RapidIngestor::PipelineComponentRegistry::Instance memory_factory(
	const RapidIngestor::PipelineComponent&, const RapidIngestor::Sink*) {
	auto* state = new MemoryState;
	std::shared_ptr<void> owner(state, [](void* raw) {
		delete static_cast<MemoryState*>(raw);
	});
	return {&memory_process, state, std::move(owner), nullptr, nullptr, nullptr,
		[](const void* raw) {
			return static_cast<const MemoryState*>(raw)->received;
		}};
}

RapidIngestor::PipelineComponentRegistry::Instance filter_factory(
	const RapidIngestor::PipelineComponent& component, const RapidIngestor::Sink*) {
	auto* state = new FilterState{
		{std::string(component.filter_field),
			std::string(component.filter_equals)}};
	std::shared_ptr<void> owner(state, [](void* raw) {
		delete static_cast<FilterState*>(raw);
	});
	return {&filter_process, state, std::move(owner), nullptr, nullptr, nullptr,
		nullptr};
}

RapidIngestor::StackLogFrame frame(std::string_view status) {
	RapidIngestor::StackLogFrame value;
	value.count = 1;
	value.elements[0] = {
		RapidIngestor::MsgPackType::String, "status", status};
	return value;
}

}

int main() {
	using namespace RapidIngestor;

	PipelineComponentRegistry components;
	components.registerType("forward", &source_process);
	components.registerType("opentelemetry", &source_process);
	components.registerFactory("filter", &filter_factory);
	components.registerFactory("memory_sink", &memory_factory);

	PipelinePortRegistry ports;
	ports.registerPort("traces", "otel.trace");

	PipelineGraph graph;
	graph.add({"source", "opentelemetry", {}});
	graph.add({"keep_errors", "filter", {"source.traces"}, "status", "error"});
	graph.add({"sink_a", "memory_sink", {"keep_errors"}});
	graph.add({"sink_b", "memory_sink", {"keep_errors"}});
	graph.validate();

	Sink sink_a;
	sink_a.id = "sink_a";
	Sink sink_b;
	sink_b.id = "sink_b";
	PipelineRouter router({sink_a, sink_b}, std::move(graph), "source",
		components, ports);

	router.add("otel.trace", frame("ok"));
	if (router.rowsInserted() != 0) {
		std::cerr << "non-matching record reached a sink\n";
		return 1;
	}

	router.add("otel.trace", frame("error"));
	if (router.rowsInserted() != 2) {
		std::cerr << "matching record did not fan out to both sinks\n";
		return 1;
	}

	std::cout << "pipeline router factory tests passed\n";
	return 0;
}
