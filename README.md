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

## Pipeline configuration

The collector accepts the legacy array configuration and a named,
Vector-style graph configuration. Named components use `inputs` references;
source output ports for OTLP are `traces`, `metrics`, and `logs`.

```json
{
  "version": 1,
  "sources": {
    "otel_in": {
      "type": "opentelemetry",
      "grpc": { "address": "127.0.0.1:4317" }
    }
  },
  "transforms": {
    "keep_errors": {
      "type": "filter",
      "inputs": ["otel_in.traces"]
    }
  },
  "sinks": {
    "clickhouse": {
      "type": "clickhouse",
      "inputs": ["keep_errors"],
      "database_table": "otel"
    }
  }
}
```

Configuration loading validates component IDs, references, OTLP output ports,
cycles, and deterministic topological ordering. Transforms are compiled and
validated but are not executed by the current runtime yet; runtime processing
still uses the existing source/sink adapter until the pipeline executor is
implemented.