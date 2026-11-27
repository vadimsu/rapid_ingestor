#!/bin/bash
set -e

export seastar_dir=../seastar
export path_to_app=$(pwd)

cmake \
  -DCMAKE_SKIP_RPATH=TRUE \
  -DCMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" \
  -DCMAKE_MODULE_PATH="$seastar_dir/cmake" \
  "$path_to_app"
cmake --build . --target otel_traffic_gen --parallel
