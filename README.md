# rapid_ingestor

The OTLP traffic generator can be built as the `otel_traffic_gen` CMake
target (or separately using `otel_traffic_gen/build.sh`). It sends one
request at a time per connection and reports a failure if the server does
not return a successful gRPC status.

Collector statistics are printed separately for each labeled listener. Row
counts can lag message counts while ClickHouse batches are being flushed.

After building `rapid_ingestor` and `otel_traffic_gen` in `build/`, run
`python3 tests/otel_integration_test.py` to test traces, metrics, logs,
connection cleanup, and failure handling without a ClickHouse server.