#!/bin/bash
set -e
cd "$(dirname "$0")"


# Build xdp-tutorial libraries first — DPDK cooking needs libxdp.a and libbpf.a
# to enable the AF_XDP PMD during Seastar's DPDK configure step.
export LIBXDP_PREFIX=$(pwd)/xdp-tutorial/lib/xdp-tools/
export PKG_CONFIG_PATH="$(pwd)/xdp-tutorial/lib/xdp-tools/lib/libxdp/libxdp.pc:$PKG_CONFIG_PATH"
cp -r xdp xdp-tutorial/filesync_kern
cd xdp-tutorial
./configure
make -C lib
cd lib/xdp-tools
./configure
make clean
make
cd ../libbpf/
make -C src clean
make -C src EXTRA_FLAGS="-fPIC"
cp src/libbpf.a ../install/lib/.
cd ../../
# Ensure libraries landed in lib/install/lib regardless of install-step behavior.
# On some machines ./configure sets SYSTEM_LIBXDP=y or the install target silently
# fails, leaving the .a files only in the xdp-tools/libbpf build directories.
mkdir -p lib/install/lib lib/install/include/xdp lib/install/include/bpf
[ -f lib/xdp-tools/lib/libxdp/libxdp.a ]  && cp -f lib/xdp-tools/lib/libxdp/libxdp.a  lib/install/lib/
[ -f lib/libbpf/src/libbpf.a ]             && cp -f lib/libbpf/src/libbpf.a             lib/install/lib/
# Headers (needed by DPDK AF_XDP PMD at configure time)
[ -d headers/xdp ]      && cp -rfu headers/xdp/.      lib/install/include/xdp/
[ -d lib/libbpf/src ]   && cp -fu  lib/libbpf/src/*.h lib/install/include/bpf/ 2>/dev/null || true
cd ..

BASEDIR=$(pwd)
cd seastar
rm -rf build
./configure.py --mode=release --without-tests --without-apps --without-demos --enable-dpdk --c++-standard=20  --cook dpdk \
    --cflags="-I${BASEDIR}/xdp-tutorial/lib/install/include"
ninja -C build/release
#sudo ninja -C build/release install
cd ..

# Build XDP kernel programs (AF_XDP BPF objects loaded at runtime)
cd xdp-tutorial
#make
cd filesync_kern
make
cd ../..
#cd build
#export seastar_dir=./seastar
#export path_to_app=$(pwd)
#cmake -DCMAKE_SKIP_RPATH=TRUE -DCMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" -DCMAKE_MODULE_PATH=$seastar_dir/cmake $path_to_app
seastar_dir=$(pwd)/seastar
path_to_app=$(pwd)
cmake -DCMAKE_SKIP_RPATH=TRUE \
	  -DCMAKE_PREFIX_PATH="${seastar_dir}/build/release;${seastar_dir}/build/release/_cooking/installed" \
	    -DCMAKE_MODULE_PATH="${seastar_dir}/cmake" \
	      "${path_to_app}"
make VERBOSE=1
