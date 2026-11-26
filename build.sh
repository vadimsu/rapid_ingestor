#/bin/bash
./reinstall_protoc.sh
./generate_proto.sh
./reinstall_absl.sh
cd nghttp2
git submodule update --init
autoreconf -i
automake
autoconf
./configure
make
cd ..
rm -f generated
./generate_proto.sh
export seastar_dir=./seastar
export path_to_app=$(pwd)
cmake -DCMAKE_SKIP_RPATH=TRUE -DCMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" -DCMAKE_MODULE_PATH=$seastar_dir/cmake $path_to_app
make VERBOSE=1
