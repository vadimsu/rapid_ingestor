#!/bin/bash
cd seastar
./configure.py --mode=release --without-tests --without-apps --without-demos --c++-standard=20
ninja -C build/release
cd ..
./build.sh
