#!/bin/bash
# Configures and builds forward_traffic_gen against the Seastar build in ../seastar.
set -e
export seastar_dir=../seastar
export path_to_app=$(pwd)
cmake -DCMAKE_SKIP_RPATH=TRUE -DCMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" -DCMAKE_MODULE_PATH=$seastar_dir/cmake $path_to_app
make VERBOSE=1
