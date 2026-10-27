# forward_traffic_gen

Seastar-based Fluent Forward protocol traffic generator. Opens N concurrent
TCP (or Unix domain) connections per core and streams MessagePack
Forward-mode messages (`[tag, [[time, record], ...]]`) toward a target
endpoint as fast as possible (or at a bounded rate), for load testing
`rapid_ingestor` or any other Fluent Forward protocol consumer.

## Building

```sh
./build.sh
```

## Basic usage

```sh
./bin/forward_traffic_gen --host 127.0.0.1 --port 24224 -c 4 \
    --connections 8 --entries-per-batch 20 --fields 6 --field-size 24
```

Without `--template`, every record is a flat map of `fields` generic string
fields (`field0`, `field1`, ...), each `field-size` bytes - this is the
historical/default record shape, good for raw throughput testing where the
receiving side doesn't care about field names or types.

## Schema templates (`--template`)

`--template <file.json>` replaces the generic fields above with an explicit,
named/typed field schema, so generated traffic matches a specific
`rapid_ingestor` ClickHouse sink configuration
(`RapidIngestor::ClickHouseSink` / `Config::Sink::columns` -
see [src/rapid_ingestor_clickhouse_sink.hh](../src/rapid_ingestor_clickhouse_sink.hh)
and [src/rapid_ingestor_config.hh](../src/rapid_ingestor_config.hh)). This
is what you want when load-testing the ClickHouse sink itself rather than
just the Forward-protocol accept/parse path.

Ready-made templates matching each of the four ClickHouse sink schema
variants in [benchmarking/](../benchmarking) are under
[templates/](./templates):

| Template | Matches sink config | Shape |
|---|---|---|
| `templates/single_column_text.json` | `config_single_column_text.json` | whole record -> one flat-text column |
| `templates/single_column_json.json` | `config_single_column_json.json` | whole record -> one JSON column |
| `templates/multi_column.json` | `config_multi_column.json` | explicit `level`/`duration_ms`/`success`/`ctx` columns |
| `templates/multi_column_aliasing.json` | `config_multi_column_aliasing.json` | explicit `lvl`/`msg`/`dur`/`ok` columns, aliased server-side |

```sh
./bin/forward_traffic_gen --host 127.0.0.1 --port 24224 -c 4 \
    --connections 8 --entries-per-batch 20 \
    --template templates/multi_column.json
```

`--fields`/`--field-size` are ignored whenever `--template` is given.

### Template file schema

```jsonc
{
  "tag": "bench.tag",     // optional, used unless --tag is passed explicitly
  "fields": [
    { "name":"level", "type":"string", "values":["debug","info","warn","error"] },
    { "name":"duration_ms", "type":"int64", "min":1, "max":5000 },
    { "name":"success", "type":"bool" },                      // alternates true/false
    { "name":"ok", "type":"bool", "value":true },              // fixed
    { "name":"ctx", "type":"map", "fields":[                  // nested record
      { "name":"user", "type":"string", "values":["alice","bob"] }
    ]}
  ]
}
```

- `type` defaults to `"string"` and is one of `string` | `int64` | `bool` | `map`.
- String fields rotate through `values` by log-entry index, or - if `values`
  is omitted - use a fixed `size`-byte dummy payload (default 16, clamped to
  31: msgpack fixstr-only, matching what
  `RapidIngestor::ProtocolEngine`/`parse_msgpack_map_zero_copy()` decodes).
- Int64 fields rotate `min..max` by log-entry index (encoded as msgpack
  int32, the only integer width the parser recognizes).
- Bool fields alternate `true`/`false` by log-entry index, or use a fixed
  `"value"`.
- Map fields nest further fields (fixmap, max 15 entries, matching the
  parser's nested-map support) - used to exercise the sink's JSON/text
  encoding of non-string field values (e.g. `config_multi_column.json`'s
  `ctx` column).
- Unknown top-level keys (e.g. `"_comment"`) are ignored.
