#!/bin/bash
sudo apt-get remove -y libabsl-dev libprotobuf-dev protobuf-compiler
sudo apt-get autoremove -y

# Install build prerequisites
sudo apt-get update && sudo apt-get install -y build-essential cmake git curl

# Download and extract Protobuf v36.2 source
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protobuf-35.0.zip
unzip protobuf-35.0.zip
cd protobuf-35.0
# Configure, build, and install both Abseil and Protobuf into /usr/local
cmake -B build \
  -DCMAKE_INSTALL_PREFIX=/usr/local \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -Dprotobuf_BUILD_TESTS=OFF

cmake --build build -j$(nproc)
sudo cmake --install build
sudo ldconfig

