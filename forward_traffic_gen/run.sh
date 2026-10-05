#!/bin/bash
# Example: 4 cores x 8 connections, 20 entries/batch, unbounded rate, against
# the TCP listener from benchmarking/config.json.
ulimit -n 65535
./bin/forward_traffic_gen --host 127.0.0.1 --port 24224 -c 8 \
    --connections 8 --entries-per-batch 20 --fields 6 --field-size 24 "$@"
