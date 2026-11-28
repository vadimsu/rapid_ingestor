#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "seastar/core/file.hh"
#include "seastar/core/seastar.hh"
#include "src/rapid_ingestor_config.hh"
#include "seastar/core/app-template.hh"

namespace {

using namespace RapidIngestor;

std::string write_config(const std::string& suffix, const std::string& equals) {
	std::string path = "/tmp/rapid_ingestor_config_pipeline_" +
		std::to_string(static_cast<long long>(::getpid())) + "_" + suffix + ".json";
	std::ofstream output(path);
	if (!output) {
		throw std::runtime_error("failed to create temporary configuration");
	}
	output << R"({
		"sources": {
			"source": {
				"type": "opentelemetry",
				"grpc": {"address": "127.0.0.1:4317"}
			}
		},
		"transforms": {
			"filter": {
				"type": "filter",
				"inputs": ["source.traces"],
				"condition": {
					"field": "value",
					"equals": )" << equals << R"(
				}
			}
		},
		"sinks": {
			"sink": {
				"type": "clickhouse",
				"inputs": ["filter"],
				"table": "events"
			}
		}
	})";
	return path;
}

seastar::future<> read_config(const std::string& path, bool expect_failure,
	PipelineFilterValueKind expected_kind) {
	auto config = seastar::make_lw_shared<Config>(path);
	return config->read().then_wrapped(
		[config, path, expect_failure, expected_kind](auto result) {
			std::remove(path.c_str());
			if (result.failed() != expect_failure) {
				return seastar::make_exception_future<>(
					std::runtime_error("configuration success/failure mismatch"));
			}
			if (result.failed()) {
				return seastar::make_ready_future<>();
			}
			const auto& components = config->getPipeline().components();
			if (components.size() != 3 ||
				components[1].filter_kind != expected_kind) {
				return seastar::make_exception_future<>(
					std::runtime_error("typed filter was not preserved"));
			}
			return seastar::make_ready_future<>();
		});
}

}

int main(int argc, char** argv) {
	seastar::app_template app;
	return app.run(argc, argv, [] {
		auto string_path = write_config("string", "\"ok\"");
		auto integer_path = write_config("integer", "42");
		auto boolean_path = write_config("boolean", "true");
		auto missing_path = write_config("missing", "null");
		auto unsupported_path = write_config("unsupported", "[]");

		return read_config(string_path, false, PipelineFilterValueKind::String)
			.then([integer_path] {
				return read_config(integer_path, false,
					PipelineFilterValueKind::Integer);
			}).then([boolean_path] {
				return read_config(boolean_path, false,
					PipelineFilterValueKind::Boolean);
			}).then([missing_path] {
				return read_config(missing_path, true,
					PipelineFilterValueKind::String);
			}).then([unsupported_path] {
				return read_config(unsupported_path, true,
					PipelineFilterValueKind::String);
			}).then_wrapped([](auto result) {
				result.get();
				std::cout << "configuration pipeline tests passed\n";
				return seastar::make_ready_future<>();
			});
	});
}
