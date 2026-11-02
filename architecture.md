Goals:
- highest possible efficiency in terms of CPU consumption for a given data rate
- highest possible efficiency in memory consumption
- highest possible throughput (theoretical limit equals theoretical limit of the underlying hardware)
- high reliability

Principles:

To reach the goals listed above, the following principles need to be followed:
- Minimize context switches
- Minimize memory copying
- Maximize cache efficiency
- Maximize algorithmic efficiency (incl using proper data structures)
- Total unblocking (no wait states)

Framework and language choice:
Seastar is chosen to serve as a framework as it naturally follows the principles listed above:
- integrates most advanced technologies for kernel bypass (io_uring, DPDK) (context switching optimization, zero-copying)
- provides an infrastructure for buffer manipulation from the point it (or fragment(s) of it) is submitted to a fill ring to the point it (or fragment(s) of it) is submitted to a transmit ring (addresses memory copying minimization)
- provides its own, more efficient implementation of future-continuation, smart pointers and coroutines mechanisms (addresses memory & cache efficiency)
- follows and encourages to follow share-nothing approach (addresses context switching minimization, memory copying minimization, cache efficiency maximization)
- fully unblocked by design

Data Input (plugins):

- Forward protocol

Data manipulation (plugins):

- Currently none

Data output (plugins):

- Clickhouse via Native protocol

Data flow:

- listener sockets are open on each shard (Seastar's terminology)
- Connections are accepted
- Data is received and stored (an array of fragments)
- Brief parsing is done to determine whether enough data is received for a record
	- If enough data - full parsing is performed. An array of views is created to point to each record/field rather than copying them
	- If not enough data - parsing stops until more data arrives; already-consumed fragments are compacted away so the buffer does not grow unbounded while waiting
- Once a record is fully parsed, it is handed to a per-shard sink callback (`ProtocolEngine::RecordSink`) set on the connection's `Protocol`; with no sink configured, records are only counted

Sharding model:

- One listener is sharded across all cores (`seastar::sharded<Listener>`), each shard independently accepting connections on the same listening address (SO_REUSEPORT-style fan-out)
- Each accepted connection is pinned to the shard that accepted it for its whole lifetime - no cross-shard hand-off, no shared state, no locking (share-nothing)
- Per-connection state (socket, receive buffer chain, parser cursor) lives entirely on its owning shard
- Aggregate stats (messages parsed, bytes processed) are collected on demand via `map_reduce` across shards; a 1s timer on the main shard polls and prints the total

Forward protocol support:

- Implements the Fluent Forward protocol (msgpack-encoded) in all three wire formats: Message mode (`[tag, time, record, options?]`), Forward mode (`[tag, entries[], options?]`) and PackedForward mode (`[tag, binary-or-string blob, options?]`)
- PackedForward blobs may be gzip-compressed (`options.compressed == "gzip"`); decompression is handled with zlib, bounded by a max decompressed size to guard against zip bombs
- Record fields are walked directly over the msgpack bytes in place (`parse_msgpack_map_zero_copy`) - keys/values are `std::string_view`s into the original network buffer, not copies

Memory management:

- Incoming bytes are appended to a `seastar::net::packet` chain as they arrive, reusing Seastar's zero-copy buffer model (no copy from the receive buffer into a separate parse buffer)
- Completeness of the next message is probed non-destructively (cursor snapshot/restore) before committing to parse it, so a partial message never corrupts parser state
- `seastar::net::packet`'s fragment deleters are chained for the lifetime of the whole packet, so trimming/consuming fragments alone does not free memory - the engine periodically compacts the unconsumed tail into a fresh packet to actually reclaim memory from already-parsed data
- Hard caps on tag length, entry counts, blob size, decompressed size and total unparsed backlog bound memory use regardless of what a (possibly adversarial) client declares; violations are treated as fatal and the connection is closed

Configuration:

- JSON config file (see benchmarking/config.json) declares `sources` (protocol: TCP/UNIX, currently TLS is parsed but not implemented) and `sinks` (ClickHouse connection: database_type/database_table/mode/host/port/dbname/username/password/target_column/encoding/columns - see below)
- Example sink configs under `benchmarking/`, one per mode:
	- `config.json` / `config_vm_loopback.json` / `config_unixdomain.json` - default single-column `raw_text` mode (unchanged `payload` column)
	- `config_single_column_text.json` - single-column mode with a renamed `target_column` and explicit flat-text `encoding`
	- `config_single_column_json.json` - single-column mode with a renamed `target_column`, `tag_map`'s `Map(String, String)` projection, and explicit JSON `encoding`
	- `config_multi_column.json` - multi-column/superset-schema mode mixing string/int64/bool columns, one aliased column, and JSON coercion of a nested-map field
	- `config_multi_column_aliasing.json` - multi-column mode focused purely on explicit aliasing (every `source` field renamed to a differently-named `target_column`)
- Only the first usable source and the first sink in the config are currently started; multiple concurrent sources/sinks are not yet wired up

Output: ClickHouse sink (native TCP protocol):

- `native_protocol.hh` is a minimal, reusable ClickHouse native-TCP-protocol client (handshake, `executeQuery` for DDL, a Block decoder, and `insertRows()` for an arbitrary ordered set of String/Int64/UInt8-typed columns)
- One `ClickHouseSink` instance is created and connected per shard (its own `ClickHouseNativeConnection`, its own pending batch) - never shared across shards, consistent with the share-nothing model
- Two insertion modes, selected by whether `Sink::columns` is configured:
	- **Single-column mode** (default, `Sink::columns` empty): rather than hand-rolling native-wire encoders for ClickHouse's `Map`/`Variant`/`Dynamic`/`JSON` column types (high-risk, version-sensitive), every mode inserts the whole record into one plain `String` column - `Sink::target_column` (defaults to `payload`) - and lets ClickHouse DDL (a `MATERIALIZED` column) project it into the fancier type server-side. Four serialization modes (`Sink::mode`) control what the target column holds and what projection is added in `ClickHouseSink::start()`:
		1. `raw_text` - simple flat `key=value,key2=value2` text, nested maps as `{k=v;k2=v2}`; no materialized column
		2. `tag_map` - JSON object text; adds `fields Map(String, String) MATERIALIZED JSONExtract(<target_column>, 'Map(String, String)')`
		3. `dynamic` - JSON object text; adds `fields Map(String, Variant(String, Int64, Float64, UInt8)) MATERIALIZED JSONExtract(...)` (targets ClickHouse 24.8+, needs `allow_experimental_variant_type`)
		4. `json` - JSON object text; adds `doc JSON MATERIALIZED <target_column>` (targets ClickHouse 24.8+, needs `allow_experimental_json_type`)
	  `Sink::encoding` (`text` | `json`) overrides the text format independently of `mode` (default: `text` for `raw_text`, `json` otherwise).
	- **Multi-column / superset-schema mode** (`Sink::columns` non-empty): the incoming record is treated as a superset of an explicit column schema. Each `Sink::columns` entry maps one record field (`source`) onto one ClickHouse column (`target_column`, defaults to `source` - i.e. explicit aliasing when they differ) of a configured `type` (`string` | `int64` | `bool`). `ClickHouseSink::start()` creates the landing table directly from this schema (`CREATE TABLE ... ORDER BY tuple()`) instead of a single payload column. On every `add()`, exactly one value is written per configured column to keep row alignment: a missing field or a type mismatch writes that type's zero value (empty string / 0 / false); a present String-column field that isn't itself a msgpack string is coerced to text via `encoding` (`text` | `json`, default `json`). Extra fields in the record that aren't in the schema are simply ignored.
- Records batch in memory per shard (row count / byte size thresholds, or a 1s timer) before a single `insertRows()` Block-encodes and sends them; a failed insert is logged and the batch is dropped (no retry/durability yet)
- Known limitations: single global sink/mode (no per-tag routing to different tables/modes yet), no reconnect-on-drop, no backpressure between the sink falling behind and the receive path


Current status / not yet implemented:

- ClickHouse sink: implemented for all 4 single-column modes plus the multi-column/superset-schema mode, but no retry/reconnect on connection loss or insert failure, no per-tag routing to multiple tables/modes, no backpressure into the receive path
- TLS listener support: not implemented (parsed from config, logged as unsupported)
- Data manipulation plugins: none yet

Testing and benchmarking:

- tests/message_parser_regression_test.cc exercises the Forward-protocol parser directly
- forward_traffic_gen/ is a standalone load generator used to drive sustained high-throughput benchmarks against the listener
- benchmarking/ holds comparison configs/scripts against Fluent Bit and Vector over both TCP and Unix domain sockets
