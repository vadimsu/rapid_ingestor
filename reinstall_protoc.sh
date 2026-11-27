#!/bin/bash
sudo apt-get remove -y protobuf-compiler

# Install unzip and curl if not already present
sudo apt-get update && sudo apt-get install -y curl unzip

# Download protoc 36.2 for Linux x86_64
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-linux-x86_64.zip

# Extract binary to /usr/local/bin and well-known .proto includes to /usr/local/include
sudo unzip -o protoc-35.0-linux-x86_64.zip -d /usr/local bin/protoc 'include/*'

# Ensure permissions are set properly
sudo chmod +x /usr/local/bin/protoc
rm protoc-35.0-linux-x86_64.zip

hash -r
which protoc && protoc --version
