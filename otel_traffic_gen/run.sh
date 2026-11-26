#!/bin/bash
set -e

ulimit -n 65535
./bin/otel_traffic_gen \
    --host 127.0.0.1 \
    --port 8888 \
    --signal traces \
    --connections 1 \
    --batches 0 \
    "$@"
