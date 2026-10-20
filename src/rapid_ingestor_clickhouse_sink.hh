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

#include <seastar/core/condition-variable.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <charconv>
#include <chrono>
#include <cstring>
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

// Max bytes std::to_chars() can produce for an int64_t: 19 digits + sign.
constexpr size_t kMaxInt64TextLen = 20;

// Appends elem.as_int() as decimal text, with no heap allocation (unlike
// std::to_string(), which allocates and destroys a temporary std::string per
// call just to copy its digits into `out` right after).
inline void appendIntAsText(std::string& out, const MsgPackElement& elem) {
	char buf[kMaxInt64TextLen];
	auto res = std::to_chars(buf, buf + sizeof(buf), elem.as_int());
	out.append(buf, res.ptr - buf);
}

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
			appendIntAsText(out, elem);
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

// Upper bound (in bytes) on the "flat text" encoding of one value - used to
// size the destination seastar::sstring exactly once up front (see
// encodeRecordFlatText()) so the actual encode can write straight into it
// through a raw cursor instead of growing a buffer incrementally. Mirrors
// writeValueFlatText() case-by-case; recursing into nested maps here is
// cheap (parse_msgpack_map_zero_copy() is just pointer arithmetic over
// already-received bytes, no allocation) so doing it twice - once to size,
// once to write - is a good trade for turning two heap allocations (a
// std::string, then a copy into an sstring) into at most one (the sstring
// itself, elided entirely by SSO for records <=14 bytes).
inline size_t flatTextValueBound(const MsgPackElement& elem) {
	switch (elem.type) {
		case MsgPackType::Integer:
			return kMaxInt64TextLen;
		case MsgPackType::Map: {
			StackLogFrame nested = parse_msgpack_map_zero_copy(elem.val);
			size_t n = 2; // '{' + '}'
			for (size_t i = 0; i < nested.count; ++i) {
				if (i) n += 1; // ';'
				n += nested.elements[i].key.size() + 1 /* '=' */;
				n += flatTextValueBound(nested.elements[i]);
			}
			return n;
		}
		default:
			return elem.val.size();
	}
}

inline size_t flatTextRecordBound(const StackLogFrame& frame) {
	size_t n = 0;
	for (size_t i = 0; i < frame.count; ++i) {
		if (i) n += 1; // ','
		n += frame.elements[i].key.size() + 1 /* '=' */;
		n += flatTextValueBound(frame.elements[i]);
	}
	return n;
}

// Writes one value in "flat text" form directly through a raw cursor - the
// buffer `dst` points into must already be sized per flatTextValueBound()/
// flatTextRecordBound(), so no bounds growth/reallocation ever happens here.
inline void writeValueFlatText(char*& dst, const MsgPackElement& elem) {
	switch (elem.type) {
		case MsgPackType::Integer: {
			auto res = std::to_chars(dst, dst + kMaxInt64TextLen, elem.as_int());
			dst = res.ptr;
			break;
		}
		case MsgPackType::Map: {
			*dst++ = '{';
			StackLogFrame nested = parse_msgpack_map_zero_copy(elem.val);
			for (size_t i = 0; i < nested.count; ++i) {
				if (i) *dst++ = ';';
				std::memcpy(dst, nested.elements[i].key.data(), nested.elements[i].key.size());
				dst += nested.elements[i].key.size();
				*dst++ = '=';
				writeValueFlatText(dst, nested.elements[i]);
			}
			*dst++ = '}';
			break;
		}
		default:
			std::memcpy(dst, elem.val.data(), elem.val.size());
			dst += elem.val.size();
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
// raw_text mode. Writes directly into a seastar::sstring sized exactly once
// up front (flatTextRecordBound()) instead of building a std::string and
// then copying it into an sstring - see detail::flatTextValueBound() for why
// that's safe with no further growth.
inline seastar::sstring encodeRecordFlatText(const StackLogFrame& frame) {
	size_t bound = detail::flatTextRecordBound(frame);
	seastar::sstring out(seastar::sstring::initialized_later(), bound);
	char* dst = out.data();
	char* const begin = dst;
	for (size_t i = 0; i < frame.count; ++i) {
		if (i) *dst++ = ',';
		std::memcpy(dst, frame.elements[i].key.data(), frame.elements[i].key.size());
		dst += frame.elements[i].key.size();
		*dst++ = '=';
		detail::writeValueFlatText(dst, frame.elements[i]);
	}
	out.resize(static_cast<size_t>(dst - begin));
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
		, _password(config.password)
		, kMaxBatchRows(config.max_batch_rows)
		, kMaxBatchBytes(config.max_batch_bytes)
		, kMaxRowsPerInsert(config.max_rows_per_insert)
		, kBackpressurePauseRows(config.backpressure_pause_rows)
		, kBackpressureResumeRows(config.backpressure_resume_rows)
		, kBackpressurePauseBytes(config.backpressure_pause_bytes)
		, kBackpressureResumeBytes(config.backpressure_resume_bytes) {
		// Reserved once; flush()'s _pendingPayloads.clear() keeps this capacity
		// around, so steady-state add() never grows/reallocates the vector.
		_pendingPayloads.reserve(kMaxBatchRows);
	}

	// Connects, creates the landing table if needed, and adds the per-mode
	// materialized projection. Must complete before add() is used.
	seastar::future<> start() {
		_conn = co_await ClickHouseNative::ClickHouseNativeConnection::connect(_host, _port, _dbname, _username, _password);
		fmt::print("Clickhouse connection established\n");
#if 1
		co_await _conn->executeQuery(fmt::format(
			"CREATE TABLE IF NOT EXISTS {} (payload String) ENGINE = MergeTree() ORDER BY (payload)",
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
		// Release anything parked in waitForCapacity() (see Protocol's read
		// loop) so a connection backpressure-paused on this sink doesn't
		// hang forever once the sink itself is shutting down.
		_capacityAvailable.broken();
		co_await flush();
		if (_conn) {
			co_await _conn->close();
		}
	}

	uint64_t getRowsInserted() const { return _rowsInserted; }

	// Backpressure: true once the pending backlog is large enough that the
	// caller (see Protocol::setBackpressure()/onAccepted()) should stop
	// reading more bytes from this shard's connections until it drains.
	// kBackpressurePauseRows/Bytes are deliberately independent of
	// kMaxBatchRows/kMaxBatchBytes (which just decide when a *normal* flush
	// is triggered) - this is the actual hard bound on memory: without it,
	// sustained input outpacing what this shard's single ClickHouse
	// connection can insert lets _pendingPayloads grow without limit, which
	// can exhaust memory even while the system overall still has free RAM
	// (seastar statically partitions memory per shard at startup, so one
	// overloaded shard can OOM independently of the others).
	bool isOverloaded() const {
		return _pendingPayloads.size() >= kBackpressurePauseRows || _pendingBytes >= kBackpressurePauseBytes;
	}

	// Resolves once the backlog has drained back below the (lower) resume
	// watermark - signaled from flush() every time it frees up capacity.
	//
	// Kicks a flush() itself rather than just waiting: once a connection is
	// paused here, add() stops being called on it entirely (no more bytes
	// are being read/parsed), so without this, the *only* thing left that
	// could ever start a new flush() and signal capacity again is the
	// kFlushInterval periodic timer - meaning a paused connection could sit
	// idle for up to a full kFlushInterval (visible as throughput
	// momentarily dropping to zero) instead of resuming as soon as this
	// shard's ClickHouse connection is actually free to take more rows.
	// flush()'s own _flushInProgress guard makes this a safe no-op if a
	// flush (from the timer, or another paused connection) is already
	// running; its synchronous prefix (move out _pendingPayloads, clear,
	// reserve, broadcast) also runs - and so unblocks waiters - in
	// microseconds, well before the actual chunked insertRows() network
	// calls even start, so this doesn't wait for a full drain to resume.
	seastar::future<> waitForCapacity() {
		(void)flush();
		co_await _capacityAvailable.wait([this] {
			return _pendingPayloads.size() < kBackpressureResumeRows && _pendingBytes < kBackpressureResumeBytes;
		});
	}

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
		if (_mode != SinkMode::RawText) {
			fmt::print("mode not supported {}\n", static_cast<int>(_mode));
			return;
		}
		// encodeRecordFlatText() now writes straight into the sstring that
		// ends up stored below, so this is the one allocation per record
		// (elided by SSO for records <=14 bytes) instead of a std::string
		// allocation followed by a copy into a second, separate sstring.
		seastar::sstring payload = encodeRecordFlatText(frame);
		_pendingBytes += payload.size();
#endif
		// Moved, not copied again, into the pending batch.
		_pendingPayloads.push_back(std::move(payload));
		// Pre-existing bug: this used to check _pendingTags.size(), which the
		// live RawText path never populates (only the dead #if no_yet branch
		// above does), so it was always 0 and the row-count cap never fired -
		// only the byte-size cap could ever trigger a flush. That let batches
		// grow far past kMaxBatchRows for small records, which is unsafe now
		// that insertRows() caps its wire fragment count (see native_protocol.hh).
		if (_pendingPayloads.size() >= kMaxBatchRows || _pendingBytes >= kMaxBatchBytes) {
			(void)flush();
		}
	}

private:
	size_t kMaxBatchRows/* = 1000000*/;
	size_t kMaxBatchBytes/* = 10ull * 1024 * 1024*/;
	static constexpr std::chrono::milliseconds kFlushInterval{1000};
	// Max rows sent in a single insertRows() call. Deliberately a *separate*
	// constant from kMaxBatchRows (which only controls when add() decides to
	// trigger a flush at all, and can reasonably be tuned far higher - e.g.
	// to mostly rely on kMaxBatchBytes/the periodic timer instead) - flush()
	// always splits whatever backlog it has into kMaxRowsPerInsert-sized
	// chunks, so a single insertRows() call's wire-fragment count never gets
	// anywhere near its guard regardless of how kMaxBatchRows is tuned.
	size_t kMaxRowsPerInsert/* = 10000*/;
	// See isOverloaded()/waitForCapacity() above.
	size_t kBackpressurePauseRows/* = 1000000*/;
	size_t kBackpressureResumeRows/* = 400000*/;
	size_t kBackpressurePauseBytes/* = 2000ull * 1024 * 1024*/;
	size_t kBackpressureResumeBytes/* = 1000ull * 1024 * 1024*/;

	seastar::future<> flush() {
		// _conn only supports one query in flight at a time; without this guard
		// the periodic timer and a batch-size-triggered flush() from add() can
		// overlap and interleave their writes/reads on the same connection,
		// desyncing the wire protocol (seen as "unexpected EOF reading VarUInt").
		if (_flushInProgress) {
			co_return;
		}
#if not_yet
		if (_pendingTags.empty() || !_conn) {
			co_return;
		}
#else
		if (_pendingPayloads.empty() || !_conn) {
			co_return;
		}
#endif
		_pendingTags.clear();
		_pendingTimestamps.clear();
		auto table = _table;
		_flushInProgress = true;

		// Keep draining as long as there's a backlog, not just the one
		// snapshot that was pending when this call started. Under
		// sustained overload (input rate persistently exceeding what this
		// shard's single ClickHouse connection can insert), anything that
		// accumulates into the *next* _pendingPayloads while this call's
		// own insertRows() awaits below would otherwise have nothing left
		// to drain it: add()'s/waitForCapacity()'s own "kick a flush" calls
		// just no-op while _flushInProgress is true, so without this loop
		// the backlog would sit idle until the next periodic timer tick
		// (or another external caller) picks it up - exactly the kind of
		// stall (observed as throughput dropping to zero) this is meant to
		// avoid.
		while (!_pendingPayloads.empty()) {
			// add()'s kMaxBatchRows check only runs on the *next* add() call, so
			// while this flush's own insertRows() co_await below is suspended,
			// more add() calls can keep growing _pendingPayloads well past
			// kMaxBatchRows. Hand off the whole backlog here, but split it
			// into kMaxRowsPerInsert-sized sub-batches sent as separate,
			// sequential insertRows() calls, so no single call's wire-fragment
			// count ever approaches insertRows()'s guard, however large the
			// backlog grew (and independent of how kMaxBatchRows is tuned).
			std::vector<seastar::sstring> pending = std::move(_pendingPayloads);
			// `pending` just took _pendingPayloads's buffer, so it has no capacity
			// left; re-reserve immediately so the next add()...add() run back up
			// to kMaxBatchRows doesn't reallocate/copy-grow the vector again.
			_pendingPayloads.clear();
			_pendingPayloads.reserve(kMaxBatchRows);
			_pendingBytes = 0;
			// _pendingPayloads/​_pendingBytes just dropped back to 0, so any
			// connection parked in waitForCapacity() can very likely proceed
			// now - wake them immediately rather than making them wait for
			// this round's (possibly many-chunk) insertRows() calls to
			// finish too.
			_capacityAvailable.broadcast();
			for (size_t offset = 0; offset < pending.size(); offset += kMaxRowsPerInsert) {
				size_t chunkRows = std::min(kMaxRowsPerInsert, pending.size() - offset);
				std::vector<seastar::sstring> chunk(
					std::make_move_iterator(pending.begin() + offset),
					std::make_move_iterator(pending.begin() + offset + chunkRows));
				ClickHouseNative::ColumnBatch batch;
				batch.rows = chunkRows;
				batch.columns.push_back(ClickHouseNative::makeStringColumn("payload", std::move(chunk)));
				co_await _conn->insertRows(table, std::move(batch)).then([this, chunkRows] {
					_rowsInserted += chunkRows;
				}).handle_exception([table, offset, chunkRows] (std::exception_ptr ep) {
					fmt::print("ClickHouse sink: insert into {} failed for rows [{}, {}): {}\n",
						table, offset, offset + chunkRows, ep);
					return seastar::make_ready_future<>();
				});
			}
		}
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
	seastar::condition_variable _capacityAvailable;
};

} // namespace RapidIngestor
