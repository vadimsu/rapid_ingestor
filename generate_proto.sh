#!/bin/bash
rm -rf generated && mkdir -p generated

/usr/local/bin/protoc \
  -I ./opentelemetry-proto \
  --cpp_out=generated \
  ./opentelemetry-proto/opentelemetry/proto/common/v1/common.proto \
  ./opentelemetry-proto/opentelemetry/proto/resource/v1/resource.proto \
  ./opentelemetry-proto/opentelemetry/proto/trace/v1/trace.proto \
  ./opentelemetry-proto/opentelemetry/proto/metrics/v1/metrics.proto \
  ./opentelemetry-proto/opentelemetry/proto/logs/v1/logs.proto \
  ./opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.proto \
  ./opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto \
  ./opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto
