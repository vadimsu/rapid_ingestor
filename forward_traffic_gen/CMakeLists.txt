cmake_minimum_required(VERSION 3.20)
project(forward_traffic_gen)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package (Seastar REQUIRED)

include_directories(
        ../seastar/include
)

add_executable (
        forward_traffic_gen
        src/forward_traffic_gen.cc
)

target_link_libraries (
        forward_traffic_gen
        PRIVATE Seastar::seastar
        )
set(CMAKE_CXX_FLAGS "-O3")
set_target_properties(forward_traffic_gen
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
