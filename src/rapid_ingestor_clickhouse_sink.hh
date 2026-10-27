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

#include <chrono>
#include <string>
#include <string_view>
#include <vector>

#include "native_protocol.hh"
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_message_parser.hh"

namespace RapidIngestor {

enum class SinkMode { RawText, TagMap, DynamicType, NativeJson };

inline SinkMode parseSinkMode(const seastar::sstring& mode) {
	if (mode == "tag_map") return SinkMode::TagMap;
	if (mode == "dynamic") return SinkMode::DynamicType;
	if (mode == "json") return SinkMode::NativeJson;
	if (!mode.empty() && mode != "raw_text") {
		fmt::print("Unknown ClickHouse sink mode '{}', defaulting to raw_text\n", mode);
	}
	return SinkMode::RawText;
}

// Controls how a record (single-column mode) or a field value coerced into a
// String column (multi-column mode, see CHColumnType below) is rendered:
// "text" is the flat "k=v;k2=v2" shape from encodeRecordFlatText()/
// encodeValueAsFlatText(), "json" is a JSON value from encodeRecordJson()/
// encodeValueAsJson().
enum class FieldEncoding { Text, Json };

inline FieldEncoding parseFieldEncoding(const seastar::sstring& encoding, FieldEncoding fallback) {
	if (encoding == "json") return FieldEncoding::Json;
	if (encoding == "text") return FieldEncoding::Text;
	if (!encoding.empty()) {
		fmt::print("Unknown ClickHouse encoding '{}', defaulting to {}\n",
			encoding, fallback == FieldEncoding::Json ? "json" : "text");
	}
	return fallback;
}

// ClickHouse column type for the explicit multi-column schema mode (see
// Sink::columns in rapid_ingestor_config.hh): one native_protocol.hh wire
// shape per type - String is VarUInt-length-prefixed bytes, Int64/Bool are
// fixed-width (8/1 bytes), matching ClickHouse's own native representation
// for Int64/UInt8.
enum class CHColumnType { String, Int64, Bool };

inline CHColumnType parseCHColumnType(const seastar::sstring& type) {
	if (type == "int64" || type == "Int64") return CHColumnType::Int64;
	if (type == "bool" || type == "Bool") return CHColumnType::Bool;
	if (!type.empty() && type != "string" && type != "String") {
		fmt::print("Unknown ClickHouse column type '{}', defaulting to String\n", type);
	}
	return CHColumnType::String;
}

inline seastar::sstring chWireTypeName(CHColumnType type) {
	switch (type) {
		case CHColumnType::Int64: return seastar::sstring("Int64");
		case CHColumnType::Bool: return seastar::sstring("UInt8");
		default: return seastar::sstring("String");
	}
}

namespace detail {

inline void jsonEscapeInto(std::string& out, std::string_view s) {
	ClickHouseNative::appendJsonEscapedString(out, seastar::sstring(s.data(), s.size()));
}

// Renders one field's value as JSON, recursing into nested maps (captured by
// parse_msgpack_map_zero_copy() as a raw, not-yet-decoded msgpack span).
inline void encodeValueAsJson(std::string& out, const MsgPackElement& elem) {
	switch (elem.type) {
		case MsgPackType::String:
			out.push_back('"');
			jsonEscapeInto(out, elem.val);
			out.push_back('"');
			break;
		case MsgPackType::Integer:{
			char buf[24];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), elem.as_int());
            out.append(buf, ptr - buf);
			break;
		}
		case MsgPackType::Map: {
			out.push_back('{');
			StackLogFrame nested = parse_msgpack_map_zero_copy(elem.val);
			for (size_t i = 0; i < nested.count; ++i) {
				if (i) out.push_back(',');
				out.push_back('"');
				jsonEscapeInto(out, nested.elements[i].key);
				out += "\":";
				encodeValueAsJson(out, nested.elements[i]);
			}
			out.push_back('}');
			break;
		}
		default:
			out += "null";
	}
}

// Renders one field's value as the "flat text" format used by RawText mode:
// same shape, just without JSON quoting/escaping - nested maps become
// "{k=v;k2=v2}". Deliberately minimal, matching the "simple string
// serialization" the mode is meant to provide.
inline void encodeValueAsFlatText(std::string& out, const MsgPackElement& elem) {
	switch (elem.type) {
		case MsgPackType::Integer:{
			char buf[24];
			auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), elem.as_int());
			out.append(buf, ptr - buf);
			break;
		}
		case MsgPackType::Map: {
			out.push_back('{');
			StackLogFrame nested = parse_msgpack_map_zero_copy(elem.val);
			for (size_t i = 0; i < nested.count; ++i) {
				if (i) out.push_back(';');
				out.append(nested.elements[i].key.data(), nested.elements[i].key.size());
				out.push_back('=');
				encodeValueAsFlatText(out, nested.elements[i]);
			}
			out.push_back('}');
			break;
		}
		default:
			out.append(elem.val.data(), elem.val.size());
	}
}

} // namespace detail

// Encodes a full record as a JSON object, e.g. {"k1":"v1","k2":42}, appending
// to an existing buffer (reused across calls to avoid per-record allocation).
// Used by tag_map/dynamic/json modes - they all insert the same JSON text,
// the DDL added in ClickHouseSink::start() is what actually projects it into
// Map/Variant/JSON columns server-side.
inline void encodeRecordJsonInto(std::string& out, const StackLogFrame& frame) {
	out.push_back('{');
	for (size_t i = 0; i < frame.count; ++i) {
		if (i) out.push_back(',');
		out.push_back('"');
		detail::jsonEscapeInto(out, frame.elements[i].key);
		out += "\":";
		detail::encodeValueAsJson(out, frame.elements[i]);
	}
	out.push_back('}');
}

inline std::string encodeRecordJson(const StackLogFrame& frame) {
	std::string out;
	encodeRecordJsonInto(out, frame);
	return out;
}

// Encodes a full record as flat "key=value,key2=value2" text. Used by
// raw_text mode.
inline void encodeRecordFlatText(std::string& out, const StackLogFrame& frame) {
	for (size_t i = 0; i < frame.count; ++i) {
		if (i) out.push_back(',');
		out.append(frame.elements[i].key.data(), frame.elements[i].key.size());
		out.push_back('=');
		detail::encodeValueAsFlatText(out, frame.elements[i]);
	}
}

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
	explicit ClickHouseSink(const Sink& config)
		: _table(config.database_table)
		, _mode(parseSinkMode(config.mode))
		, _targetColumn(config.target_column)
		, _encoding(parseFieldEncoding(config.encoding,
			_mode == SinkMode::RawText ? FieldEncoding::Text : FieldEncoding::Json))
		, _host(config.host)
		, _port(seastar::to_sstring(config.port))
		, _dbname(config.dbname)
		, _username(config.username)
		, _password(config.password)
		, _maxBatchBytes(config.max_batch_bytes)
		, _backpressurePauseBytes(config.backpressure_pause_bytes)
		, _backpressureResumeBytes(config.backpressure_resume_bytes)
		, _pendingRows(0)
		, _pendingBytes(0) {
			_payloadColumnDataIdx = 0;
			_payloadColumnData[0] = seastar::sstring();
			_payloadColumnData[0].reserve(_maxBatchBytes + 65536);
			_payloadColumnData[1] = seastar::sstring();
			_payloadColumnData[1].reserve(_maxBatchBytes + 65536);
			_scratchBuffer.reserve(65536);
			if (!config.columns.empty()) {
				// Multi-column superset schema: config.columns is a subset of
				// the input record's fields, each mapped onto its own
				// ClickHouse column (with optional aliasing/type coercion).
				size_t reserve_bytes = (_maxBatchBytes / config.columns.size()) + 65536;
				_columns.reserve(config.columns.size());
				for (auto& c : config.columns) {
					_columns.emplace_back();
					ColumnState& cs = _columns.back();
					cs.source = c.source;
					cs.target = c.target_column.empty() ? c.source : c.target_column;
					cs.type = parseCHColumnType(c.type);
					cs.encoding = parseFieldEncoding(c.encoding, FieldEncoding::Json);
					cs.reserve(reserve_bytes);
				}
			}
	}

	// Connects, creates the landing table if needed, and adds the per-mode
	// materialized projection. Must complete before add() is used.
	seastar::future<> start() {
		_conn = co_await ClickHouseNative::ClickHouseNativeConnection::connect(_host, _port, _dbname, _username, _password);
		fmt::print("Clickhouse connection established\n");

		if (!_columns.empty()) {
			// Multi-column mode: the table's schema is the explicit `columns`
			// list - the input record is expected to be a superset of it, not
			// the other way round, so there's no per-mode JSON/Map projection
			// here (unlike single-column mode below).
			std::string columnDefs;
			for (size_t i = 0; i < _columns.size(); ++i) {
				if (i) columnDefs += ", ";
				columnDefs.append(_columns[i].target.data(), _columns[i].target.size());
				columnDefs += ' ';
				auto wireType = chWireTypeName(_columns[i].type);
				columnDefs.append(wireType.data(), wireType.size());
			}
			co_await _conn->executeQuery(fmt::format(
				"CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree() ORDER BY tuple()",
				_table, columnDefs));
			fmt::print("clickhouse table created ({} columns)\n", _columns.size());
		} else {
			co_await _conn->executeQuery(fmt::format(
				"CREATE TABLE IF NOT EXISTS {} ({} String) ENGINE = MergeTree() ORDER BY ({})",
				_table, _targetColumn, _targetColumn));
			fmt::print("clickhouse table created\n");
			switch (_mode) {
				case SinkMode::RawText:
					fmt::print("raw text\n");
					break;
				case SinkMode::TagMap:
					co_await _conn->executeQuery(fmt::format(
						"ALTER TABLE {} ADD COLUMN IF NOT EXISTS fields Map(String, String) "
						"MATERIALIZED JSONExtract({}, 'Map(String, String)')",
						_table, _targetColumn));
					break;
				case SinkMode::DynamicType:
					// Session-scoped setting: stays enabled on `_conn` for every later
					// query (including the inserts below), since it's the same connection.
					co_await _conn->executeQuery("SET allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1");
					co_await _conn->executeQuery(fmt::format(
						"ALTER TABLE {} ADD COLUMN IF NOT EXISTS fields Map(String, Variant(String, Int64, Float64, UInt8)) "
						"MATERIALIZED JSONExtract({}, 'Map(String, Variant(String, Int64, Float64, UInt8))')",
						_table, _targetColumn));
					break;
				case SinkMode::NativeJson:
					co_await _conn->executeQuery("SET allow_experimental_json_type = 1");
					co_await _conn->executeQuery(fmt::format(
						"ALTER TABLE {} ADD COLUMN IF NOT EXISTS doc JSON MATERIALIZED {}",
						_table, _targetColumn));
					break;
			}
		}
		fmt::print("starting timer to flush\n");
		_flushTimer.set_callback([this] { (void)flush(); });
		_flushTimer.arm_periodic(kFlushInterval);
		fmt::print("timer to flush started\n");
	}

	seastar::future<> stop() {
		_flushTimer.cancel();
		_capacityAvailable.broadcast();
		co_await flush();
		if (_conn) {
			co_await _conn->close();
		}
	}

	uint64_t getRowsInserted() const { return _rowsInserted; }

	// Synchronous: encodes the record into this shard's pending batch and
	// returns immediately. `frame`'s string_views must not be retained past
	// this call - they point into transient parser buffers.
	void add(std::string_view tag, const StackLogFrame& frame) {
		if (!_columns.empty()) {
			addColumns(frame);
			return;
		}
		_scratchBuffer.clear();
		if (_encoding == FieldEncoding::Json) {
			encodeRecordJsonInto(_scratchBuffer, frame);
		} else {
			encodeRecordFlatText(_scratchBuffer, frame);
		}
		auto size = _scratchBuffer.size();
		ClickHouseNative::putVarUInt(_payloadColumnData[_payloadColumnDataIdx], _scratchBuffer.size());
	        _payloadColumnData[_payloadColumnDataIdx].append(std::move(_scratchBuffer));
		_pendingRows++;
		_pendingBytes += size;
		if (_pendingBytes >= _maxBatchBytes) {
			(void)flush();
		}
	}

	bool isOverloaded() const {
		return _pendingBytes >= _backpressurePauseBytes;
	}

	seastar::future<> waitForCapacity() {
		(void)flush();
		co_await _capacityAvailable.wait([this] {
			return _pendingBytes < _backpressureResumeBytes;
		});
	}

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
	void addColumns(const StackLogFrame& frame) {
		size_t bytes_added = 0;
		for (auto& col : _columns) {
			std::string& buf = col.buffers[_payloadColumnDataIdx];
			size_t before = buf.size();
			auto opt_elem = frame.find(col.source);
			switch (col.type) {
				case CHColumnType::String:
					if (opt_elem) {
						if (opt_elem->type == MsgPackType::String) {
							ClickHouseNative::putString(buf, opt_elem->val);
						} else {
							_scratchBuffer.clear();
							if (col.encoding == FieldEncoding::Json) {
								detail::encodeValueAsJson(_scratchBuffer, *opt_elem);
							} else {
								detail::encodeValueAsFlatText(_scratchBuffer, *opt_elem);
							}
							ClickHouseNative::putString(buf, std::string_view(_scratchBuffer));
						}
					} else {
						ClickHouseNative::putVarUInt(buf, 0); // missing field: empty string
					}
					break;
				case CHColumnType::Int64:
					if (opt_elem && opt_elem->type == MsgPackType::Integer) {
						ClickHouseNative::putInt64(buf, opt_elem->as_int());
					} else {
						ClickHouseNative::putInt64(buf, 0); // missing or type mismatch
					}
					break;
				case CHColumnType::Bool: {
					uint8_t b = 0;
					if (opt_elem && opt_elem->type == MsgPackType::Boolean && !opt_elem->val.empty()) {
						b = (opt_elem->val[0] != 0) ? 1 : 0;
					}
					buf.push_back(static_cast<char>(b));
					break;
				}
			}
			bytes_added += buf.size() - before;
		}
		_pendingRows++;
		_pendingBytes += bytes_added;
		if (_pendingBytes >= _maxBatchBytes) {
			(void)flush();
		}
	}

	seastar::future<> flush() {
		// _conn only supports one query in flight at a time; without this guard
		// the periodic timer and a batch-size-triggered flush() from add() can
		// overlap and interleave their writes/reads on the same connection,
		// desyncing the wire protocol (seen as "unexpected EOF reading VarUInt").
		if (_flushInProgress) {
			co_return;
		}
		if (!_conn){
			co_return;
		}

		ClickHouseNative::ColumnBatch batch;
		batch.rows = _pendingRows;
		auto idx = _payloadColumnDataIdx;
		_payloadColumnDataIdx = 1 - _payloadColumnDataIdx;

		if (_columns.empty()) {
			_payloadColumnData[_payloadColumnDataIdx].clear();
			batch.columns.push_back(ClickHouseNative::makePrebakedStringColumn(
				seastar::sstring(_targetColumn), std::move(_payloadColumnData[idx])));
		} else {
			// Attach all pre-baked per-column buffers to the batch and clear
			// them (capacity intact, zero reallocations) for the next cycle.
			for (auto& col : _columns) {
				switch (col.type) {
					case CHColumnType::String:
						batch.columns.push_back(ClickHouseNative::makePrebakedStringColumn(
							seastar::sstring(col.target), std::move(col.buffers[idx])));
						break;
					case CHColumnType::Int64:
						batch.columns.push_back(ClickHouseNative::makePrebakedIntColumn(
							seastar::sstring(col.target), seastar::sstring("Int64"), std::move(col.buffers[idx])));
						break;
					case CHColumnType::Bool:
						batch.columns.push_back(ClickHouseNative::makePrebakedBoolColumn(
							seastar::sstring(col.target), std::move(col.buffers[idx])));
						break;
				}
				col.buffers[idx].clear();
			}
		}

		_pendingRows = 0;

		auto table = _table;
		_flushInProgress = true;
		_capacityAvailable.broadcast();
		co_await _conn->insertRows(_table, std::move(batch)).then([this, rows=batch.rows] {
			_rowsInserted += rows;
		}).handle_exception([table] (std::exception_ptr ep) {
			fmt::print("ClickHouse sink: insert into {} failed: {}\n", table, ep);
			return seastar::make_ready_future<>();
		});
		_pendingBytes = 0;//after as it serves backpressure
		_flushInProgress = false;
	}

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
