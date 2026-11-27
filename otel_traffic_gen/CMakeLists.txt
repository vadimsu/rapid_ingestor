cmake_minimum_required(VERSION 3.20)
project(otel_traffic_gen)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Seastar REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

include_directories(
        ../seastar/include
        ../generated
        ../opentelemetry-proto/gen/cpp
        ../nghttp2/lib/includes
)

link_directories(../nghttp2/lib/.libs)

add_executable(
        otel_traffic_gen
        src/otel_traffic_gen.cc
        ../generated/opentelemetry/proto/common/v1/common.pb.cc
        ../generated/opentelemetry/proto/resource/v1/resource.pb.cc
        ../generated/opentelemetry/proto/trace/v1/trace.pb.cc
        ../generated/opentelemetry/proto/metrics/v1/metrics.pb.cc
        ../generated/opentelemetry/proto/logs/v1/logs.pb.cc
        ../generated/opentelemetry/proto/collector/trace/v1/trace_service.pb.cc
        ../generated/opentelemetry/proto/collector/metrics/v1/metrics_service.pb.cc
        ../generated/opentelemetry/proto/collector/logs/v1/logs_service.pb.cc
)

target_link_libraries(
        otel_traffic_gen
        PRIVATE Seastar::seastar
        PRIVATE protobuf::libprotobuf
        PRIVATE libnghttp2.a
)

set(CMAKE_CXX_FLAGS "-O3")
set_target_properties(
        otel_traffic_gen
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)
