#include <iostream>

#include "src/rapid_ingestor_listener.hh"

namespace {

struct Counter {
	int value = 0;
};

void forward_record(RapidIngestor::PipelineExecutionPlan& plan, size_t node,
	void*, std::string_view tag, const RapidIngestor::StackLogFrame& frame) {
	plan.emit(node, tag, frame);
}

void count_record(RapidIngestor::PipelineExecutionPlan&,
	size_t, void* state, std::string_view, const RapidIngestor::StackLogFrame&) {
	++static_cast<Counter*>(state)->value;
}

RapidIngestor::StackLogFrame make_frame(std::string_view value) {
	RapidIngestor::StackLogFrame frame;
	frame.count = 1;
	frame.elements[0] = {
		RapidIngestor::MsgPackType::String, "status", value};
	return frame;
}

}

int main() {
	using namespace RapidIngestor;

	PipelineStringFilter filter{"status", "error"};
	if (!filter.matches(make_frame("error")) ||
		filter.matches(make_frame("ok"))) {
		std::cerr << "filter predicate test failed\n";
		return 1;
	}

	alignas(int32_t) static const char integer_bytes[] = {0, 0, 0, 42};
	StackLogFrame integer_frame;
	integer_frame.count = 1;
	integer_frame.elements[0] = {
		MsgPackType::Integer, "attempts",
		std::string_view(integer_bytes, sizeof(integer_bytes))};
	PipelineStringFilter integer_filter;
	integer_filter.field = "attempts";
	integer_filter.kind = PipelineFilterValueKind::Integer;
	integer_filter.expected_integer = 42;
	if (!integer_filter.matches(integer_frame)) {
		std::cerr << "integer filter predicate test failed\n";
		return 1;
	}

	static const char true_byte = 1;
	StackLogFrame boolean_frame;
	boolean_frame.count = 1;
	boolean_frame.elements[0] = {
		MsgPackType::Boolean, "enabled", std::string_view(&true_byte, 1)};
	PipelineStringFilter boolean_filter;
	boolean_filter.field = "enabled";
	boolean_filter.kind = PipelineFilterValueKind::Boolean;
	boolean_filter.expected_boolean = true;
	if (!boolean_filter.matches(boolean_frame)) {
		std::cerr << "boolean filter predicate test failed\n";
		return 1;
	}

	PipelineExecutionPlan plan;
	Counter untagged;
	Counter traces;
	Counter metrics;
	auto source = plan.addNode(&forward_record, nullptr);
	auto untagged_sink = plan.addNode(&count_record, &untagged);
	auto traces_sink = plan.addNode(&count_record, &traces);
	auto metrics_sink = plan.addNode(&count_record, &metrics);

	plan.connect(source, untagged_sink);
	plan.connect(source, traces_sink, "traces");
	plan.indexTaggedEdge(source, "otel.trace");
	plan.connect(source, metrics_sink, "metrics");
	plan.indexTaggedEdge(source, "otel.metrics");

	auto frame = make_frame("error");
	plan.dispatch(source, "otel.trace", frame);
	plan.dispatch(source, "otel.metrics", frame);

	if (untagged.value != 2 || traces.value != 1 || metrics.value != 1) {
		std::cerr << "tagged fan-out test failed\n";
		return 1;
	}

	std::cout << "pipeline execution plan tests passed\n";
	return 0;
}
