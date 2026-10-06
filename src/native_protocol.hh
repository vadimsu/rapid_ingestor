#pragma once

// Minimal ClickHouse native TCP protocol client for Seastar.
//
// Implements just enough of the wire protocol (see
// https://clickhouse.com/docs/reference/interfaces/specs/NativeProtocol and
// https://clickhouse.com/docs/reference/interfaces/specs/NativeFormat) to run
// DDL statements and bulk-insert arbitrary (name, type, values) columns
// non-blockingly.
//
// The client deliberately advertises a protocol version below
// DBMS_MIN_REVISION_WITH_CLIENT_INFO (54032). Since the negotiated version is
// min(client, server), this keeps every later feature gate (ClientInfo,
// per-column has_custom_serialization, ServerHello timezone/display_name,
// interserver secrets, ...) inactive, so both the handshake and the Block
// wire format stay in their oldest/simplest shape. ClickHouse servers remain
// wire-compatible with such old client revisions.
//
// Scope: only the column types actually produced by the code generator
// (UInt64, String, Bool, and the other fixed-width integer/float types for
// decoding simple SELECT results in tests) are supported; arbitrary
// composite types (Array/Tuple/Nullable/LowCardinality/...) are not decoded
// and raise an exception if encountered.

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace RapidIngestor {
namespace ClickHouseNative {

// Kept below DBMS_MIN_REVISION_WITH_CLIENT_INFO (54032) - see file header note.
constexpr uint64_t kClientProtocolVersion = 54031;

enum class ClientPacket : uint64_t {
	Hello = 0,
	Query = 1,
	Data = 2,
	Ping = 4,
};

enum class ServerPacket : uint64_t {
	Hello = 0,
	Data = 1,
	Exception = 2,
	Progress = 3,
	Pong = 4,
	EndOfStream = 5,
	ProfileInfo = 6,
	Totals = 7,
	Extremes = 8,
	TablesStatusResponse = 9,
	Log = 10,
	TableColumns = 11,
	ProfileEvents = 14,
	TimezoneUpdate = 17,
};

struct ClickHouseServerException : public std::runtime_error {
	int32_t code;
	ClickHouseServerException(int32_t code_, const std::string& msg)
		: std::runtime_error(msg), code(code_) {}
};

// ---------------------------------------------------------------------------
// Wire primitives - writers append to a byte buffer, readers pull from a
// seastar::input_stream<char>.
// ---------------------------------------------------------------------------

inline void putVarUInt(std::string& out, uint64_t value) {
	do {
		uint8_t b = value & 0x7f;
		value >>= 7;
		if (value) {
			b |= 0x80;
		}
		out.push_back(static_cast<char>(b));
	} while (value);
}

inline void putString(std::string& out, const seastar::sstring& s) {
	putVarUInt(out, s.size());
	out.append(s.data(), s.size());
}

inline void putUInt64(std::string& out, uint64_t v) {
	for (int i = 0; i < 8; ++i) {
		out.push_back(static_cast<char>(v & 0xff));
		v >>= 8;
	}
}

inline void putInt32(std::string& out, int32_t v) {
	uint32_t u = static_cast<uint32_t>(v);
	for (int i = 0; i < 4; ++i) {
		out.push_back(static_cast<char>(u & 0xff));
		u >>= 8;
	}
}

// Number of bytes putVarUInt() would write for `value` - used to compute the
// exact wire size of a Block up front so the output buffer can be reserve()'d
// once instead of growing/reallocating as fields are appended.
inline size_t varUIntSize(uint64_t value) {
	size_t n = 1;
	while (value >>= 7) {
		++n;
	}
	return n;
}

// Appends `s` to `out` with JSON string escaping (quotes/backslashes/control
// characters). Lets generated Flush() code build payload JSON directly into
// a reused buffer instead of going through nlohmann::json's DOM/serializer.
inline void appendJsonEscapedString(std::string& out, const seastar::sstring& s) {
	for (size_t i = 0; i < s.size(); ++i) {
		unsigned char c = static_cast<unsigned char>(s[i]);
		switch (c) {
			case '"': out += "\\\""; break;
			case '\\': out += "\\\\"; break;
			case '\n': out += "\\n"; break;
			case '\r': out += "\\r"; break;
			case '\t': out += "\\t"; break;
			default:
				if (c < 0x20) {
					char buf[7];
					std::snprintf(buf, sizeof(buf), "\\u%04x", c);
					out += buf;
				} else {
					out.push_back(static_cast<char>(c));
				}
		}
	}
}

// One column of an insert batch: a name, its ClickHouse wire type name (must
// match a type decodeScalar()/the server understands - String or an
// unsigned/signed fixed-width integer), and a closure that appends this
// column's values (in row order) onto the Block being built. Keeping the
// encoding behind a closure lets insertRows() stay agnostic of the
// column's underlying value type (seastar::sstring vs uint64_t, ...).
struct Column {
	seastar::sstring name;
	seastar::sstring type;
	size_t encoded_size = 0; // bytes encode() will append - precomputed so the caller can reserve() once
	std::function<void(std::string&)> encode;
};

// Columnar batch of an arbitrary, caller-defined set of columns, built once
// per flush and encoded directly into a single Block by insertRows(). All
// columns must have the same number of rows (`rows`).
struct ColumnBatch {
	std::vector<Column> columns;
	size_t rows = 0;

	bool empty() const { return rows == 0; }
};

// Builds a String column from `values` (copied/moved in): each row is
// VarUInt-length-prefixed bytes, ClickHouse type "String".
inline Column makeStringColumn(seastar::sstring name, std::vector<seastar::sstring>& values) {
	size_t size = 0;
	for (auto& v : values) {
		size += varUIntSize(v.size()) + v.size();
	}
	Column col;
	col.name = std::move(name);
	col.type = seastar::sstring("String");
	col.encoded_size = size;
	col.encode = [values = std::move(values)] (std::string& out) {
		for (auto& v : values) {
			putString(out, v);
		}
	};
	return col;
}

// Builds a fixed-width 8-byte unsigned column (ClickHouse "UInt64",
// "DateTime64(N)", ... - anything whose native wire encoding is a plain
// 8-byte little-endian integer).
inline Column makeUInt64Column(seastar::sstring name, seastar::sstring type, std::vector<uint64_t> values) {
	Column col;
	col.encoded_size = values.size() * 8;
	col.name = std::move(name);
	col.type = std::move(type);
	col.encode = [values = std::move(values)] (std::string& out) {
		for (auto v : values) {
			putUInt64(out, v);
		}
	};
	return col;
}

// A single ClickHouse-native connection. Not safe for concurrent use: the
// protocol is stateful and processes one query at a time, so callers must
// await each execute/insert call before issuing the next one (the same
// assumption the HTTP-based ClickHouseStorage client makes about its
// seastar::http client).
class ClickHouseNativeConnection : public seastar::enable_lw_shared_from_this<ClickHouseNativeConnection> {
public:
	static seastar::future<seastar::lw_shared_ptr<ClickHouseNativeConnection>> connect(
			seastar::sstring host, seastar::sstring port, seastar::sstring dbname,
			seastar::sstring username, seastar::sstring password) {
		auto addr = seastar::socket_address(seastar::ipv4_addr(
				std::string(host.c_str(), host.size()), static_cast<uint16_t>(atoi(port.c_str()))));
		auto fd = co_await seastar::connect(addr);
		fd.set_nodelay(true);
		auto conn = seastar::make_lw_shared<ClickHouseNativeConnection>(std::move(fd));
		co_await conn->handshake(dbname, username, password);
		co_return conn;
	}

	explicit ClickHouseNativeConnection(seastar::connected_socket fd)
		: _fd(std::move(fd)), _in(_fd.input()), _out(_fd.output()) {
	}

	seastar::future<> close() {
		// Keep the connection (and its streams) alive for the duration of the
		// close, even if the caller's last reference to us is dropped as soon
		// as this call is issued (e.g. a continuation chain releasing its
		// lw_shared_ptr right after kicking off close()).
		auto self = shared_from_this();
		return _out.close().then_wrapped([self] (seastar::future<> f) {
			f.ignore_ready_future();
			return self->_in.close();
		});
	}

	// Runs a DDL/utility statement to completion. Any rows the server sends
	// back are discarded unless `outRows` is provided (used by tests to read
	// back simple scalar results such as `SELECT count() ...`).
	seastar::future<> executeQuery(seastar::sstring query, std::vector<std::vector<seastar::sstring>>* outRows = nullptr) {
		std::string buf;
		appendQueryPacket(buf, nextQueryId(), query);
		appendEmptyDataMarker(buf);
		co_await _out.write(buf);
		co_await _out.flush();
		co_await drainResponses(outRows);
	}

	// Bulk-inserts (event, payload) rows into `table` (fixed one-column shape:
	// ``payload String`) using the INSERT phase of the
	// protocol: Query, empty external-tables terminator ("go" signal - required
	// even though we have no external tables, exactly like executeQuery()),
	// drain metadata until the schema block, send one Data block with the
	// rows, then the empty end-of-input marker.
	//
	// NOTE: table/batch are taken BY VALUE (not by reference) deliberately.
	// This is a coroutine - a reference parameter would only extend the
	// referred-to object's lifetime if the *caller* keeps it alive across
	// every co_await here. Callers that invoke this from inside a `.then()`
	// continuation (rather than co_await'ing it directly from another
	// coroutine) have their closure - and anything it captured by reference -
	// destroyed as soon as the call expression returns the coroutine's
	// future, which is long before the coroutine itself finishes running.
	// Taking these by value copies/moves the data into this coroutine's own
	// frame at the call site, so it stays valid for the whole call.
	//
	// `batch.columns` drives both the INSERT's explicit column list and the
	// Block sent for it, in order - this generalizes what used to be a
	// hardcoded (event UInt64, payload String) shape to whatever columns the
	// caller built with makeStringColumn()/makeUInt64Column().
	seastar::future<> insertRows(seastar::sstring table, ColumnBatch batch) {
		if (batch.empty()) {
			co_return;
		}
		std::string queryText = "INSERT INTO " + table + " (";
		for (size_t i = 0; i < batch.columns.size(); ++i) {
			if (i) {
				queryText += ", ";
			}
			queryText.append(batch.columns[i].name.data(), batch.columns[i].name.size());
		}
		queryText += ") VALUES";
		std::string queryBuf;
		queryBuf.reserve(queryText.size() + 32);
		appendQueryPacket(queryBuf, nextQueryId(), queryText);
		appendEmptyDataMarker(queryBuf);
		co_await _out.write(queryBuf);
		co_await _out.flush();

		// Drain metadata packets until the schema Data block (a header block:
		// columns present, 0 rows). We already know the target shape matches
		// what we are about to send, so the schema contents themselves are
		// discarded - only the byte-stream position matters.
		for (;;) {
			auto packetType = co_await getVarUInt();
			if (packetType == static_cast<uint64_t>(ServerPacket::Data)) {
				co_await getString(); // block name (external-table marker, unused)
				co_await decodeBlock();
				break;
			}
			co_await handleMetadataPacket(packetType);
		}

		const size_t numRows = batch.rows;

		// Compute the exact encoded size up front (mirrors appendDefaultBlockInfo's
		// fixed 8-byte shape: two 1-byte field markers + 1-byte is_overflows +
		// 4-byte bucket_number + 1-byte terminator) so dataBuf is allocated once,
		// instead of growing/reallocating repeatedly as fields are appended below.
		size_t expectedSize = varUIntSize(static_cast<uint64_t>(ClientPacket::Data));
		expectedSize += varUIntSize(0); // empty block-name string
		expectedSize += 8; // appendDefaultBlockInfo()
		expectedSize += varUIntSize(batch.columns.size());
		expectedSize += varUIntSize(numRows);
		for (const auto& col : batch.columns) {
			expectedSize += varUIntSize(col.name.size()) + col.name.size();
			expectedSize += varUIntSize(col.type.size()) + col.type.size();
			expectedSize += col.encoded_size;
		}

		std::string dataBuf;
		dataBuf.reserve(expectedSize);
		putVarUInt(dataBuf, static_cast<uint64_t>(ClientPacket::Data));
		putString(dataBuf, seastar::sstring());
		appendDefaultBlockInfo(dataBuf);
		putVarUInt(dataBuf, batch.columns.size());
		putVarUInt(dataBuf, numRows);
		for (const auto& col : batch.columns) {
			putString(dataBuf, col.name);
			putString(dataBuf, col.type);
			col.encode(dataBuf);
		}
		co_await _out.write(dataBuf);

		std::string endBuf;
		endBuf.reserve(16);
		appendEmptyDataMarker(endBuf);
		co_await _out.write(endBuf);
		co_await _out.flush();

		co_await drainResponses(nullptr);
	}

private:
	struct Block {
		std::vector<std::pair<seastar::sstring, seastar::sstring>> columns;
		std::vector<std::vector<seastar::sstring>> rows;
	};

	seastar::connected_socket _fd;
	seastar::input_stream<char> _in;
	seastar::output_stream<char> _out;

	// Query IDs must be unique across the whole server, not just this
	// connection: each shard owns its own sink/connection, so a per-connection
	// counter (e.g. "dl-1", "dl-2", ...) collides as soon
	// as two shards send a query around the same time, and ClickHouse rejects
	// the second one with "Query with id = ... is already running." Sending
	// an empty query_id lets the server assign its own globally-unique id
	// (the same thing clickhouse-client does when --query_id isn't given).
	seastar::sstring nextQueryId() {
		return seastar::sstring();
	}

	static void appendDefaultBlockInfo(std::string& buf) {
		putVarUInt(buf, 1);
		buf.push_back(0); // is_overflows = 0
		putVarUInt(buf, 2);
		putInt32(buf, -1); // bucket_number = -1
		putVarUInt(buf, 0); // terminator
	}

	static void appendQueryPacket(std::string& buf, const seastar::sstring& queryId, const seastar::sstring& query) {
		putVarUInt(buf, static_cast<uint64_t>(ClientPacket::Query));
		putString(buf, queryId);
		putString(buf, seastar::sstring()); // settings list terminator: no settings
		// QueryProcessingStage: for the old (pre-54032) protocol generation this
		// client speaks, the only stages are FetchColumns=0, WithMergeableState=1,
		// Complete=2 (later protocol revisions added extra intermediate stages
		// and renumbered Complete to 4, but that only matters to newer clients).
		putVarUInt(buf, 2); // stage = Complete
		putVarUInt(buf, 0); // compression = disabled
		putString(buf, query);
	}

	static void appendEmptyDataMarker(std::string& buf) {
		putVarUInt(buf, static_cast<uint64_t>(ClientPacket::Data));
		putString(buf, seastar::sstring());
		appendDefaultBlockInfo(buf);
		putVarUInt(buf, 0); // num_columns
		putVarUInt(buf, 0); // num_rows
	}

	seastar::future<> handshake(seastar::sstring dbname, seastar::sstring username, seastar::sstring password) {
		std::string buf;
		putVarUInt(buf, static_cast<uint64_t>(ClientPacket::Hello));
		putString(buf, seastar::sstring("distributed_logger"));
		putVarUInt(buf, 1); // version_major
		putVarUInt(buf, 1); // version_minor
		putVarUInt(buf, kClientProtocolVersion);
		putString(buf, dbname);
		putString(buf, username);
		putString(buf, password);
		co_await _out.write(buf);
		co_await _out.flush();

		auto packetType = co_await getVarUInt();
		if (packetType == static_cast<uint64_t>(ServerPacket::Exception)) {
			co_await throwException();
		}
		if (packetType != static_cast<uint64_t>(ServerPacket::Hello)) {
			throw std::runtime_error("ClickHouse native: unexpected packet type " +
					std::to_string(packetType) + " during handshake");
		}
		co_await getString(); // server_name
		co_await getVarUInt(); // server version_major
		co_await getVarUInt(); // server version_minor
		co_await getVarUInt(); // server protocol_version
		// No timezone/display_name/version_patch/...: those ServerHello fields
		// are gated at revisions above kClientProtocolVersion (see file header).
	}

	seastar::future<> throwException() {
		auto code = co_await getFixedInt32();
		auto name = co_await getString();
		auto message = co_await getString();
		co_await getString(); // stack_trace
		co_await _in.read_exactly(1); // has_nested (obsolete)
		throw ClickHouseServerException(code, std::string(name.c_str(), name.size()) +
				": " + std::string(message.c_str(), message.size()));
	}

	seastar::future<> handleMetadataPacket(uint64_t packetType) {
		switch (packetType) {
			case static_cast<uint64_t>(ServerPacket::Progress):
				co_await getVarUInt();
				co_await getVarUInt();
				co_await getVarUInt();
				break;
			case static_cast<uint64_t>(ServerPacket::ProfileInfo):
				co_await getVarUInt();
				co_await getVarUInt();
				co_await getVarUInt();
				co_await _in.read_exactly(1);
				co_await getVarUInt();
				co_await _in.read_exactly(1);
				break;
			case static_cast<uint64_t>(ServerPacket::Log):
			case static_cast<uint64_t>(ServerPacket::ProfileEvents):
				co_await getString(); // block name (external-table marker, unused)
				co_await decodeBlock();
				break;
			case static_cast<uint64_t>(ServerPacket::TableColumns):
				co_await getString();
				co_await getString();
				break;
			case static_cast<uint64_t>(ServerPacket::TimezoneUpdate):
				co_await getString();
				break;
			case static_cast<uint64_t>(ServerPacket::Exception):
				co_await throwException();
				break;
			default:
				throw std::runtime_error("ClickHouse native: unexpected packet type " +
						std::to_string(packetType) + " while awaiting INSERT schema");
		}
	}

	seastar::future<> drainResponses(std::vector<std::vector<seastar::sstring>>* outRows) {
		for (;;) {
			auto packetType = co_await getVarUInt();
			switch (packetType) {
				case static_cast<uint64_t>(ServerPacket::Data):
				case static_cast<uint64_t>(ServerPacket::Totals):
				case static_cast<uint64_t>(ServerPacket::Extremes):
				case static_cast<uint64_t>(ServerPacket::Log):
				case static_cast<uint64_t>(ServerPacket::ProfileEvents): {
					co_await getString(); // block name (external-table marker, unused)
					auto block = co_await decodeBlock();
					if (packetType == static_cast<uint64_t>(ServerPacket::Data) && outRows != nullptr) {
						for (auto& row : block.rows) {
							outRows->push_back(std::move(row));
						}
					}
					break;
				}
				case static_cast<uint64_t>(ServerPacket::Progress):
					co_await getVarUInt();
					co_await getVarUInt();
					co_await getVarUInt();
					break;
				case static_cast<uint64_t>(ServerPacket::ProfileInfo):
					co_await getVarUInt();
					co_await getVarUInt();
					co_await getVarUInt();
					co_await _in.read_exactly(1);
					co_await getVarUInt();
					co_await _in.read_exactly(1);
					break;
				case static_cast<uint64_t>(ServerPacket::TableColumns):
					co_await getString();
					co_await getString();
					break;
				case static_cast<uint64_t>(ServerPacket::TimezoneUpdate):
					co_await getString();
					break;
				case static_cast<uint64_t>(ServerPacket::EndOfStream):
					co_return;
				case static_cast<uint64_t>(ServerPacket::Exception):
					co_await throwException();
					co_return; // unreachable: throwException() always throws
				default:
					throw std::runtime_error("ClickHouse native: unexpected packet type " +
							std::to_string(packetType));
			}
		}
	}

	seastar::future<Block> decodeBlock() {
		for (;;) {
			auto fieldId = co_await getVarUInt();
			if (fieldId == 0) {
				break;
			}
			if (fieldId == 1) {
				co_await _in.read_exactly(1); // is_overflows
			} else if (fieldId == 2) {
				co_await getFixedInt32(); // bucket_number
			} else {
				throw std::runtime_error("ClickHouse native: unknown BlockInfo field " + std::to_string(fieldId));
			}
		}
		auto numColumns = co_await getVarUInt();
		auto numRows = co_await getVarUInt();
		Block block;
		block.columns.reserve(numColumns);
		if (numColumns > 0 && numRows > 0) {
			block.rows.assign(numRows, std::vector<seastar::sstring>(numColumns));
		}
		for (uint64_t c = 0; c < numColumns; ++c) {
			auto name = co_await getString();
			auto type = co_await getString();
			block.columns.emplace_back(name, type);
			for (uint64_t r = 0; r < numRows; ++r) {
				block.rows[r][c] = co_await decodeScalar(type);
			}
		}
		co_return block;
	}

	seastar::future<seastar::sstring> decodeScalar(const seastar::sstring& typeStr) {
		std::string type(typeStr.c_str(), typeStr.size());
		auto paren = type.find('(');
		std::string base = paren == std::string::npos ? type : type.substr(0, paren);

		if (base == "String") {
			co_return co_await getString();
		}
		if (base == "UInt8" || base == "Bool" || base == "Enum8") {
			auto b = co_await _in.read_exactly(1);
			co_return seastar::to_sstring(static_cast<unsigned>(static_cast<uint8_t>(b.get()[0])));
		}
		if (base == "Int8") {
			auto b = co_await _in.read_exactly(1);
			co_return seastar::to_sstring(static_cast<int>(static_cast<int8_t>(b.get()[0])));
		}
		if (base == "UInt16" || base == "Date" || base == "Enum16") {
			auto v = co_await getFixedUInt(2);
			co_return seastar::to_sstring(static_cast<unsigned>(v));
		}
		if (base == "Int16") {
			auto v = co_await getFixedUInt(2);
			co_return seastar::to_sstring(static_cast<int>(static_cast<int16_t>(v)));
		}
		if (base == "UInt32" || base == "DateTime" || base == "Date32") {
			auto v = co_await getFixedUInt(4);
			co_return seastar::to_sstring(static_cast<uint32_t>(v));
		}
		if (base == "Int32") {
			auto v = co_await getFixedUInt(4);
			co_return seastar::to_sstring(static_cast<int32_t>(v));
		}
		if (base == "Float32") {
			auto b = co_await _in.read_exactly(4);
			float f;
			std::memcpy(&f, b.get(), 4);
			co_return seastar::to_sstring(f);
		}
		if (base == "UInt64") {
			auto v = co_await getFixedUInt(8);
			co_return seastar::to_sstring(v);
		}
		if (base == "Int64" || base == "DateTime64") {
			auto v = co_await getFixedUInt(8);
			co_return seastar::to_sstring(static_cast<int64_t>(v));
		}
		if (base == "Float64") {
			auto b = co_await _in.read_exactly(8);
			double d;
			std::memcpy(&d, b.get(), 8);
			co_return seastar::to_sstring(d);
		}
		throw std::runtime_error("ClickHouse native: unsupported column type '" + type + "'");
	}

	seastar::future<uint64_t> getVarUInt() {
		uint64_t result = 0;
		int shift = 0;
		for (;;) {
			auto buf = co_await _in.read_exactly(1);
			if (buf.size() != 1) {
				throw std::runtime_error("ClickHouse native: unexpected EOF reading VarUInt");
			}
			uint8_t b = static_cast<uint8_t>(buf.get()[0]);
			result |= static_cast<uint64_t>(b & 0x7f) << shift;
			if (!(b & 0x80)) {
				break;
			}
			shift += 7;
		}
		co_return result;
	}

	seastar::future<seastar::sstring> getString() {
		auto len = co_await getVarUInt();
		if (len == 0) {
			co_return seastar::sstring();
		}
		auto buf = co_await _in.read_exactly(len);
		if (buf.size() != len) {
			throw std::runtime_error("ClickHouse native: unexpected EOF reading String");
		}
		co_return seastar::sstring(buf.get(), buf.size());
	}

	seastar::future<uint64_t> getFixedUInt(size_t width) {
		auto buf = co_await _in.read_exactly(width);
		if (buf.size() != width) {
			throw std::runtime_error("ClickHouse native: unexpected EOF reading fixed-width integer");
		}
		uint64_t v = 0;
		for (size_t i = 0; i < width; ++i) {
			v |= static_cast<uint64_t>(static_cast<uint8_t>(buf.get()[i])) << (8 * i);
		}
		co_return v;
	}

	seastar::future<int32_t> getFixedInt32() {
		auto v = co_await getFixedUInt(4);
		co_return static_cast<int32_t>(static_cast<uint32_t>(v));
	}
};

} // namespace ClickHouseNative
} // namespace RapidIngestor
