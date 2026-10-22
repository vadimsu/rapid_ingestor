#pragma once

// ClickHouse output sink: serializes parsed Forward-protocol records and
// bulk-inserts them via the native TCP protocol (native_protocol.hh).
//
// Supports four record serialization modes (Config::Sink::mode), all of
// which insert into the very same (tag String, ts UInt64, payload String)
// landing table using only wire types native_protocol.hh already knows how
// to encode (String, UInt64) - only the `payload` text format and an extra
// server-side materialized column (DDL run once in start()) differ between
// modes. This avoids hand-rolling native-wire encoders for ClickHouse's
// Map/Variant/Dynamic/JSON column types, which are version-sensitive;
// instead ClickHouse itself projects the JSON/text payload into those types.
//
//   raw_text - payload is flat "key=value,key2=value2" text, no projection.
//   tag_map  - payload is JSON text; `fields Map(String, String)` MATERIALIZED.
//   dynamic  - payload is JSON text; `fields Map(String, Variant(...))` MATERIALIZED.
//   json     - payload is JSON text; `doc JSON` MATERIALIZED (needs CH >= 24.8).

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

// Encodes a full record as a JSON object, e.g. {"k1":"v1","k2":42}. Used by
// tag_map/dynamic/json modes - they all insert the same JSON text, the DDL
// added in ClickHouseSink::start() is what actually projects it into
// Map/Variant/JSON columns server-side.
inline std::string encodeRecordJson(const StackLogFrame& frame) {
	std::string out;
	out.push_back('{');
	for (size_t i = 0; i < frame.count; ++i) {
		if (i) out.push_back(',');
		out.push_back('"');
		detail::jsonEscapeInto(out, frame.elements[i].key);
		out += "\":";
		detail::encodeValueAsJson(out, frame.elements[i]);
	}
	out.push_back('}');
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

// Bulk-inserts parsed Forward-protocol records into ClickHouse over the
// native TCP protocol. One instance per shard (own connection, own pending
// batch) - never shared across shards, matching Seastar's share-nothing model.
class ClickHouseSink : public seastar::enable_lw_shared_from_this<ClickHouseSink> {
public:
	explicit ClickHouseSink(const Sink& config)
		: _table(config.database_table)
		, _mode(parseSinkMode(config.mode))
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
			_payloadColumnData = seastar::sstring();
			_payloadColumnData.reserve(_maxBatchBytes + 65536);
	}

	// Connects, creates the landing table if needed, and adds the per-mode
	// materialized projection. Must complete before add() is used.
	seastar::future<> start() {
		_conn = co_await ClickHouseNative::ClickHouseNativeConnection::connect(_host, _port, _dbname, _username, _password);
		fmt::print("Clickhouse connection established\n");

		co_await _conn->executeQuery(fmt::format(
			"CREATE TABLE IF NOT EXISTS {} (payload String) ENGINE = MergeTree() ORDER BY (payload)",
			_table));
		fmt::print("clickhouse table created\n");
		switch (_mode) {
			case SinkMode::RawText:
				fmt::print("raw text\n");
				break;
			case SinkMode::TagMap:
				co_await _conn->executeQuery(fmt::format(
					"ALTER TABLE {} ADD COLUMN IF NOT EXISTS fields Map(String, String) "
					"MATERIALIZED JSONExtract(payload, 'Map(String, String)')",
					_table));
				break;
			case SinkMode::DynamicType:
				// Session-scoped setting: stays enabled on `_conn` for every later
				// query (including the inserts below), since it's the same connection.
				co_await _conn->executeQuery("SET allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1");
				co_await _conn->executeQuery(fmt::format(
					"ALTER TABLE {} ADD COLUMN IF NOT EXISTS fields Map(String, Variant(String, Int64, Float64, UInt8)) "
					"MATERIALIZED JSONExtract(payload, 'Map(String, Variant(String, Int64, Float64, UInt8))')",
					_table));
				break;
			case SinkMode::NativeJson:
				co_await _conn->executeQuery("SET allow_experimental_json_type = 1");
				co_await _conn->executeQuery(fmt::format(
					"ALTER TABLE {} ADD COLUMN IF NOT EXISTS doc JSON MATERIALIZED payload",
					_table));
				break;
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
		_scratchBuffer.clear();
		if (_mode == SinkMode::RawText){
			encodeRecordFlatText(_scratchBuffer, frame);
		}else{
			fmt::print("mode not supported {}\n",static_cast<int>(_mode));
			return;
		}
		ClickHouseNative::putVarUInt(_payloadColumnData, _scratchBuffer.size());
        _payloadColumnData.append(_scratchBuffer);
		_pendingRows++;
		_pendingBytes += _scratchBuffer.size();
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

	seastar::future<> flush() {
		// _conn only supports one query in flight at a time; without this guard
		// the periodic timer and a batch-size-triggered flush() from add() can
		// overlap and interleave their writes/reads on the same connection,
		// desyncing the wire protocol (seen as "unexpected EOF reading VarUInt").
		if (_flushInProgress) {
			co_return;
		}
		ClickHouseNative::ColumnBatch batch;
		batch.rows = _pendingRows;

		std::string column_data = std::move(_payloadColumnData);

		if (!_conn){
			co_return;
		}
		_payloadColumnData = std::string();
    	_payloadColumnData.reserve(_maxBatchBytes + 65536); // Pre-allocate!
    	_pendingRows = 0;
		batch.columns.push_back(ClickHouseNative::makePrebakedStringColumn("payload", std::move(column_data)));

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
	seastar::sstring _host;
	seastar::sstring _port;
	seastar::sstring _dbname;
	seastar::sstring _username;
	seastar::sstring _password;
	seastar::lw_shared_ptr<ClickHouseNative::ClickHouseNativeConnection> _conn;

	std::string _payloadColumnData;
    std::string _scratchBuffer;
	size_t _pendingRows = 0;
	size_t _pendingBytes = 0;
	seastar::timer<> _flushTimer;
	uint64_t _rowsInserted = 0;
	bool _flushInProgress = false;
	seastar::condition_variable _capacityAvailable;
};

} // namespace RapidIngestor
