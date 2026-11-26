cmake_minimum_required(VERSION 3.20)
project(rapid_ingestor)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package (Seastar REQUIRED)
find_package (Valgrind)
find_package(ZLIB REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

include(FindPkgConfig)

include_directories(
        seastar/include
	json/include
	nghttp2/lib/includes
	generated
	opentelemetry-proto/gen/cpp
)
link_directories(nghttp2/lib/.libs)

add_executable (
        rapid_ingestor
	src/rapid_ingestor_listener.cc
	src/rapid_ingestor_forward_protocol.cc
	src/rapid_ingestor_otel.cc
	src/rapid_ingestor_nghttp2_callbacks.cc
	src/rapid_ingestor_clickhouse_sink.cc
	generated/opentelemetry/proto/common/v1/common.pb.cc
	generated/opentelemetry/proto/resource/v1/resource.pb.cc
	generated/opentelemetry/proto/trace/v1/trace.pb.cc
	generated/opentelemetry/proto/metrics/v1/metrics.pb.cc
	generated/opentelemetry/proto/logs/v1/logs.pb.cc
	generated/opentelemetry/proto/collector/logs/v1/logs_service.pb.cc
	generated/opentelemetry/proto/collector/trace/v1/trace_service.pb.cc
	generated/opentelemetry/proto/collector/metrics/v1/metrics_service.pb.cc
        src/rapid_ingestor.cc
)

target_link_libraries (
        rapid_ingestor
	PRIVATE libnghttp2.a
	PRIVATE ZLIB::ZLIB
        PRIVATE Seastar::seastar
	PRIVATE protobuf::libprotobuf
	)
        set(CMAKE_CXX_FLAGS "-O3 -DSEASTAR_SSTRING -DSEASTAR_HEAPPROF")
set_target_properties(rapid_ingestor
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
