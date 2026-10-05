cmake_minimum_required(VERSION 3.20)
project(rapid_ingestor)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package (Seastar REQUIRED)
find_package (Valgrind)
find_package(ZLIB REQUIRED)

include(FindPkgConfig)

include_directories(
        seastar/include
	json/include
)
link_directories(
)

add_executable (
        rapid_ingestor
        src/rapid_ingestor.cc
)

target_link_libraries (
        rapid_ingestor
	PRIVATE ZLIB::ZLIB
        PRIVATE Seastar::seastar
        )
        set(CMAKE_CXX_FLAGS "-O3 -DSEASTAR_SSTRING -DSEASTAR_HEAPPROF")
set_target_properties(rapid_ingestor
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
