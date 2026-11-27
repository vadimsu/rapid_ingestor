#!/bin/bash
printf '\x00\x00\x00\x00\x0a\x0a\x08\x12\x06\x12\x04\x2a\x02hi' | curl -v --http2-prior-knowledge \
  -H "content-type: application/grpc" \
  -H "te: trailers" \
  --data-binary @- \
  http://127.0.0.1:8888/opentelemetry.proto.collector.trace.v1.TraceService/Export
