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

#define not_yet 0

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
		case MsgPackType::Integer:
			out += std::to_string(elem.as_int());
			break;
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
		case MsgPackType::Integer:
			out += std::to_string(elem.as_int());
			break;
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
inline std::string encodeRecordFlatText(const StackLogFrame& frame) {
	std::string out;
	for (size_t i = 0; i < frame.count; ++i) {
		if (i) out.push_back(',');
		out.append(frame.elements[i].key.data(), frame.elements[i].key.size());
		out.push_back('=');
		detail::encodeValueAsFlatText(out, frame.elements[i]);
	}
	return out;
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
		, _password(config.password) {
	}

	// Connects, creates the landing table if needed, and adds the per-mode
	// materialized projection. Must complete before add() is used.
	seastar::future<> start() {
		_conn = co_await ClickHouseNative::ClickHouseNativeConnection::connect(_host, _port, _dbname, _username, _password);
		fmt::print("Clickhouse connection established\n");
#if not_yet
		co_await _conn->executeQuery(fmt::format(
			"CREATE TABLE IF NOT EXISTS {} (tag String, ts UInt64, payload String) ENGINE = MergeTree() ORDER BY (tag, ts)",
			_table));
#else
#endif
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
#if no_yet
		std::string payload = (_mode == SinkMode::RawText) ? encodeRecordFlatText(frame) : encodeRecordJson(frame);
		uint64_t ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
			std::chrono::system_clock::now().time_since_epoch()).count();
		_pendingBytes += tag.size() + payload.size();
		_pendingTags.emplace_back(tag.data(), tag.size());
		_pendingTimestamps.push_back(ts_ns);
#else
		seastar::sstring payload;
		if (_mode == SinkMode::RawText){
			std::string flat = encodeRecordFlatText(frame);
			payload = seastar::sstring(flat.data(), flat.size());
			_pendingBytes += payload.size();
		}else{
			fmt::print("mode not supported {}\n",static_cast<int>(_mode));
			return;
		}
#endif
		_pendingPayloads.emplace_back(payload.data(), payload.size());
		if (_pendingTags.size() >= kMaxBatchRows || _pendingBytes >= kMaxBatchBytes) {
			(void)flush();
		}
	}

private:
	static constexpr size_t kMaxBatchRows = 2000;
	static constexpr size_t kMaxBatchBytes = 4ull * 1024 * 1024;
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
#if not_yet
		if (_pendingTags.empty() || !_conn) {
			co_return;
		}
		batch.rows = _pendingTags.size();
#endif
#if not_yet
		batch.columns.push_back(ClickHouseNative::makeStringColumn("tag", std::move(_pendingTags)));
		batch.columns.push_back(ClickHouseNative::makeUInt64Column("ts", "UInt64", std::move(_pendingTimestamps)));
		batch.columns.push_back(ClickHouseNative::makeStringColumn("payload", std::move(_pendingPayloads)));
#else
		if (_pendingPayloads.empty() || !_conn){
			co_return;
		}
		batch.rows = _pendingPayloads.size();
		batch.columns.push_back(ClickHouseNative::makeStringColumn("payload", _pendingPayloads));
#endif
		size_t rows = batch.rows;
		_pendingTags.clear();
		_pendingTimestamps.clear();
		_pendingPayloads.clear();
		_pendingBytes = 0;
		auto table = _table;
		_flushInProgress = true;
		co_await _conn->insertRows(_table, std::move(batch)).then([this, rows] {
			_rowsInserted += rows;
		}).handle_exception([table] (std::exception_ptr ep) {
			fmt::print("ClickHouse sink: insert into {} failed: {}\n", table, ep);
			return seastar::make_ready_future<>();
		});
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

	std::vector<seastar::sstring> _pendingTags;
	std::vector<uint64_t> _pendingTimestamps;
	std::vector<seastar::sstring> _pendingPayloads;
	size_t _pendingBytes = 0;
	seastar::timer<> _flushTimer;
	uint64_t _rowsInserted = 0;
	bool _flushInProgress = false;
};

} // namespace RapidIngestor
