#pragma once

// ClickHouse output sink: serializes parsed Forward-protocol records and
// bulk-inserts them via the native TCP protocol (native_protocol.hh).
//
// Two insertion modes, selected by whether Config::Sink::columns is set:
//
//  - Single-column mode (Sink::columns empty, the default): the whole record
//    is serialized into one configurable String column (Sink::target_column,
//    defaults to "payload"). Four serialization modes (Config::Sink::mode)
//    control what that column holds and what extra server-side materialized
//    column (DDL run once in start()) projects it into - this avoids
//    hand-rolling native-wire encoders for ClickHouse's Map/Variant/Dynamic/
//    JSON column types, which are version-sensitive; instead ClickHouse
//    itself projects the JSON/text column into those types:
//      raw_text - column is flat "key=value,key2=value2" text, no projection.
//      tag_map  - column is JSON text; `fields Map(String, String)` MATERIALIZED.
//      dynamic  - column is JSON text; `fields Map(String, Variant(...))` MATERIALIZED.
//      json     - column is JSON text; `doc JSON` MATERIALIZED (needs CH >= 24.8).
//    Sink::encoding ("text" | "json") can override the column's text format
//    independently of `mode` (default: raw_text -> text, else -> json).
//
//  - Multi-column / superset-schema mode (Sink::columns non-empty): the
//    incoming record is expected to be a superset of an explicit column
//    schema. Each Sink::columns entry maps one record field onto one
//    ClickHouse column (String/Int64/Bool), with optional aliasing (the
//    ClickHouse column name may differ from the field name) and, for String
//    columns, optional text/JSON coercion of non-string field values. See
//    ColumnState/addColumns() below.

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/condition-variable.hh>

#include <chrono>
#include <string>
#include <string_view>
#include <vector>
#include <exception>
#include <iostream>

#include "native_protocol.hh"
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_message_parser.hh"

namespace RapidIngestor {

enum class SinkMode { RawText, TagMap, DynamicType, NativeJson };

// Controls how a record (single-column mode) or a field value coerced into a
// String column (multi-column mode, see CHColumnType below) is rendered:
// "text" is the flat "k=v;k2=v2" shape from encodeRecordFlatText()/
// encodeValueAsFlatText(), "json" is a JSON value from encodeRecordJson()/
// encodeValueAsJson().
enum class FieldEncoding { Text, Json };

// ClickHouse column type for the explicit multi-column schema mode (see
// Sink::columns in rapid_ingestor_config.hh): one native_protocol.hh wire
// shape per type - String is VarUInt-length-prefixed bytes, Int64/Bool are
// fixed-width (8/1 bytes), matching ClickHouse's own native representation
// for Int64/UInt8.
enum class CHColumnType { String, Int64, Bool };

// One explicit field->column mapping for the multi-column schema mode (see
// Sink::columns): `source` is the StackLogFrame field name looked up on each
// add(), `target` is the ClickHouse column name it's written to - these
// differ when the config aliases a field to a differently-named column.
// `buffers` is double-buffered exactly like ClickHouseSink's single-column
// payload buffer, flipped in lockstep with it on every flush().
struct ColumnState {
	seastar::sstring source;
	seastar::sstring target;
	CHColumnType type = CHColumnType::String;
	FieldEncoding encoding = FieldEncoding::Json;
	std::string buffers[2];

	void reserve(size_t bytes) {
		buffers[0].reserve(bytes);
		buffers[1].reserve(bytes);
	}
};

// Bulk-inserts parsed Forward-protocol records into ClickHouse over the
// native TCP protocol. One instance per shard (own connection, own pending
// batch) - never shared across shards, matching Seastar's share-nothing model.
class ClickHouseSink : public seastar::enable_lw_shared_from_this<ClickHouseSink> {
public:
	explicit ClickHouseSink(const Sink& config);

	// Connects, creates the landing table if needed, and adds the per-mode
	// materialized projection. Must complete before add() is used.
	seastar::future<> start();

	seastar::future<> stop();

	uint64_t getRowsInserted() const;

	// Synchronous: encodes the record into this shard's pending batch and
	// returns immediately. `frame`'s string_views must not be retained past
	// this call - they point into transient parser buffers.
	void add(std::string_view tag, const StackLogFrame& frame);

	bool isOverloaded() const;

	seastar::future<> waitForCapacity();
private:
	size_t _maxBatchBytes = 10ull * 1024 * 1024;
	size_t _backpressurePauseBytes = 8ull * 1024 * 1024;
	size_t _backpressureResumeBytes = 4ull * 1024 * 1024;
	static constexpr std::chrono::milliseconds kFlushInterval{1000};

	// Multi-column mode: writes exactly one value for every configured
	// column, preserving row alignment even when `frame` is missing a field
	// (writes the type's zero value: empty string / 0 / false) or has it
	// under a type that doesn't match the column (coerced to text/JSON for
	// String columns, zero for Int64/Bool).
	void addColumns(const StackLogFrame& frame);

	seastar::future<> flush();

	seastar::sstring _table;
	SinkMode _mode;
	seastar::sstring _targetColumn;
	FieldEncoding _encoding;
	seastar::sstring _host;
	seastar::sstring _port;
	seastar::sstring _dbname;
	seastar::sstring _username;
	seastar::sstring _password;
	seastar::lw_shared_ptr<ClickHouseNative::ClickHouseNativeConnection> _conn;
	std::string _payloadColumnData[2];
	int _payloadColumnDataIdx;
	std::string _scratchBuffer;
	std::vector<ColumnState> _columns;
	size_t _pendingRows = 0;
	size_t _pendingBytes = 0;
	seastar::timer<> _flushTimer;
	uint64_t _rowsInserted = 0;
	bool _flushInProgress = false;
	seastar::condition_variable _capacityAvailable;
};

} // namespace RapidIngestor
