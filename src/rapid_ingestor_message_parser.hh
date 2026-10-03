#pragma once
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>
#include <string_view>
#include <iostream>
#include <optional>
#include <bit>
#include <vector>
#include "zlib.h"

namespace RapidIngestor {
enum class TelemetrySignal : uint8_t { Logs = 0, Metrics = 1, Traces = 2 };
enum class ForwardFormat     : uint8_t { Message, Forward, PackedForward };
constexpr size_t MAX_FIELDS_PER_LOG = 32;

// Guard limits against misbehaving/adversarial clients: these keep a single
// connection's memory use bounded no matter what lengths it declares.
constexpr size_t MAX_TAG_LEN             = 256;
constexpr size_t MAX_OPTION_STRING_LEN    = 4096;
constexpr size_t MAX_ENTRIES_PER_FORWARD  = 1'000'000;
constexpr size_t MAX_PACKED_BLOB_BYTES    = 64ull * 1024 * 1024;   // single PackedForward chunk
constexpr size_t MAX_DECOMPRESSED_BYTES   = 256ull * 1024 * 1024;  // zip-bomb guard
constexpr size_t MAX_PENDING_BYTES        = 32ull * 1024 * 1024;   // unparsed backlog per connection

enum class MsgPackType : uint8_t { Integer, String, Map, Array, Boolean };

struct MsgPackElement {
    MsgPackType type;
    std::string_view key;
    std::string_view val;
    int64_t as_int() const {
        if (val.size() == 1) return static_cast<int8_t>(val[0]);
        if (val.size() == 2) return std::byteswap(*reinterpret_cast<const int16_t*>(val.data()));
        if (val.size() == 4) return std::byteswap(*reinterpret_cast<const int32_t*>(val.data()));
        return std::byteswap(*reinterpret_cast<const int64_t*>(val.data()));
    }
};

struct StackLogFrame {
    std::array<MsgPackElement, MAX_FIELDS_PER_LOG> elements;
    size_t count = 0;
    std::optional<MsgPackElement> find(std::string_view key) const {
        for (size_t i = 0; i < count; ++i)
            if (elements[i].key == key) return elements[i];
        return std::nullopt;
    }
};

StackLogFrame parse_msgpack_map_zero_copy(std::string_view raw_buffer) {
    StackLogFrame frame;
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(raw_buffer.data());
    const uint8_t* end = ptr + raw_buffer.size();
    if (ptr >= end || (*ptr & 0xF0) != 0x80) return frame;
    size_t map_size = *ptr & 0x0F; ptr++;
    for (size_t i = 0; i < map_size && ptr < end && frame.count < MAX_FIELDS_PER_LOG; ++i) {
        if ((*ptr & 0xE0) != 0xA0) break;
        size_t key_len = *ptr & 0x1F; ptr++;
        std::string_view key(reinterpret_cast<const char*>(ptr), key_len); ptr += key_len;
        MsgPackElement elem; elem.key = key;
        if ((*ptr & 0xE0) == 0xA0) {
            elem.type = MsgPackType::String;
            size_t str_len = *ptr & 0x1F; ptr++;
            elem.val = std::string_view(reinterpret_cast<const char*>(ptr), str_len); ptr += str_len;
        } else if (*ptr == 0xD2) {
            elem.type = MsgPackType::Integer; ptr++;
            elem.val = std::string_view(reinterpret_cast<const char*>(ptr), 4); ptr += 4;
        } else if ((*ptr & 0xF0) == 0x80) {
            elem.type = MsgPackType::Map;
            const uint8_t* start_map = ptr;
            size_t inner = *ptr & 0x0F; ptr++;
            for (size_t j = 0; j < inner * 2; ++j) {
                if ((*ptr & 0xE0) == 0xA0) ptr += ((*ptr & 0x1F) + 1);
                else ptr++;
            }
            elem.val = std::string_view(reinterpret_cast<const char*>(start_map), ptr - start_map);
        }
        frame.elements[frame.count++] = elem;
    }
    return frame;
}

// ---------------------------------------------------------------------------
// PacketCursor: lightweight base — remaining, offset, peek_byte, consume,
//               peek_and_skip_object, consume_msgpack_header_and_get_len,
//               try_skip_object (for completeness probing), save/restore state.
// ---------------------------------------------------------------------------
class PacketCursor {
public:
    // Snapshot for non-destructive probing (used by fullPacketReceived)
    struct Snapshot {
        size_t frag_idx;
        size_t frag_offset;
        size_t global_offset;
    };

    PacketCursor(const PacketCursor& other): _packet_chain(other._packet_chain) {
	    _frag_idx = other._frag_idx;
	    _frag_offset = other._frag_offset;
	    _global_offset = other._global_offset;
    }

    PacketCursor(seastar::net::packet& packet_chain)
        : _packet_chain(packet_chain) {}

    size_t remaining() const { return _packet_chain.len() - _global_offset; }
    size_t offset()    const { return _global_offset; }

    int peek_byte(uint8_t& dst, size_t ahead = 0) const {
        if (_packet_chain.len() == 0)                    return 1;
        if (_frag_idx >= _packet_chain.nr_frags())       return 1;
        const auto& frag = _packet_chain.frag(_frag_idx);
        if (frag.size < 1)                               return 1;
        if (_frag_offset + ahead >= frag.size)           return 1;
        dst = reinterpret_cast<const uint8_t*>(frag.base)[_frag_offset + ahead];
        return 0;
    }

    void consume(size_t len) {
        _global_offset += len;
        _frag_offset   += len;
        while (_frag_idx < _packet_chain.nr_frags() &&
               _frag_offset >= _packet_chain.frag(_frag_idx).size) {
            _frag_offset -= _packet_chain.frag(_frag_idx).size;
            _frag_idx++;
        }
    }

    // Zeroes bookkeeping only — used after the referenced packet_chain has
    // been replaced wholesale (e.g. ProtocolEngine::compact()).
    void rebase() {
        _frag_idx      = 0;
        _frag_offset   = 0;
        _global_offset = 0;
    }

    // Tries to skip one MsgPack object. Returns false if not enough data.
    bool try_skip_object() {
        if (remaining() == 0) return false;
        uint8_t type = 0;
        if (peek_byte(type, 0)) return false;

        // Positive/Negative FixInt
        if ((type & 0x80) == 0 || (type & 0xE0) == 0xE0) { consume(1); return true; }
        // FixStr
        if ((type & 0xE0) == 0xA0) {
            size_t len = type & 0x1F;
            if (remaining() < 1 + len) return false;
            consume(1 + len); return true;
        }
        // FixArray
        if ((type & 0xF0) == 0x90) {
            size_t n = type & 0x0F; consume(1);
            for (size_t i = 0; i < n; ++i) if (!try_skip_object()) return false;
            return true;
        }
        // FixMap
        if ((type & 0xF0) == 0x80) {
            size_t n = type & 0x0F; consume(1);
            for (size_t i = 0; i < n * 2; ++i) if (!try_skip_object()) return false;
            return true;
        }
        switch (type) {
            case 0xC0: case 0xC2: case 0xC3: consume(1); return true;
            case 0xCC: case 0xD0: if (remaining() < 2) return false; consume(2); return true;
            case 0xCD: case 0xD1: if (remaining() < 3) return false; consume(3); return true;
            case 0xCE: case 0xD2: case 0xCA: if (remaining() < 5) return false; consume(5); return true;
            case 0xCF: case 0xD3: case 0xCB: if (remaining() < 9) return false; consume(9); return true;
            case 0xD9: case 0xC4: { // str8 / bin8
                if (remaining() < 2) return false; consume(1);
                uint8_t l = 0; if (peek_byte(l, 0)) return false; consume(1);
                if (remaining() < l) return false; consume(l); return true;
            }
            case 0xDA: case 0xC5: { // str16 / bin16
                if (remaining() < 3) return false; consume(1);
                uint8_t b1 = 0, b2 = 0;
                if (peek_byte(b1, 0)) return false; consume(1);
                if (peek_byte(b2, 0)) return false; consume(1);
                uint16_t l = (uint16_t(b1) << 8) | b2;
                if (remaining() < l) return false; consume(l); return true;
            }
            case 0xDB: case 0xC6: { // str32 / bin32
                if (remaining() < 5) return false; consume(1);
                uint8_t b[4] = {};
                for (int i = 0; i < 4; ++i) { if (peek_byte(b[i], 0)) return false; consume(1); }
                uint32_t l = (uint32_t(b[0])<<24)|(uint32_t(b[1])<<16)|(uint32_t(b[2])<<8)|b[3];
                if (remaining() < l) return false; consume(l); return true;
            }
            case 0xDC: { // array16
                if (remaining() < 3) return false; consume(1);
                uint8_t b1=0, b2=0;
                if (peek_byte(b1,0)) return false; consume(1);
                if (peek_byte(b2,0)) return false; consume(1);
                uint16_t n = (uint16_t(b1)<<8)|b2;
                for (size_t i = 0; i < n; ++i) if (!try_skip_object()) return false;
                return true;
            }
            case 0xDD: { // array32
                if (remaining() < 5) return false; consume(1);
                uint8_t b[4] = {};
                for (int i = 0; i < 4; ++i) { if (peek_byte(b[i],0)) return false; consume(1); }
                uint32_t n = (uint32_t(b[0])<<24)|(uint32_t(b[1])<<16)|(uint32_t(b[2])<<8)|b[3];
                for (size_t i = 0; i < n; ++i) if (!try_skip_object()) return false;
                return true;
            }
            case 0xDE: { // map16
                if (remaining() < 3) return false; consume(1);
                uint8_t b1=0, b2=0;
                if (peek_byte(b1,0)) return false; consume(1);
                if (peek_byte(b2,0)) return false; consume(1);
                uint16_t n = (uint16_t(b1)<<8)|b2;
                for (size_t i = 0; i < n*2; ++i) if (!try_skip_object()) return false;
                return true;
            }
            case 0xDF: { // map32
                if (remaining() < 5) return false; consume(1);
                uint8_t b[4] = {};
                for (int i = 0; i < 4; ++i) { if (peek_byte(b[i],0)) return false; consume(1); }
                uint32_t n = (uint32_t(b[0])<<24)|(uint32_t(b[1])<<16)|(uint32_t(b[2])<<8)|b[3];
                for (size_t i = 0; i < n*2; ++i) if (!try_skip_object()) return false;
                return true;
            }
            case 0xD4: if (remaining() < 3)  return false; consume(3);  return true;
            case 0xD5: if (remaining() < 4)  return false; consume(4);  return true;
            case 0xD6: if (remaining() < 6)  return false; consume(6);  return true;
            case 0xD7: if (remaining() < 10) return false; consume(10); return true;
            case 0xD8: if (remaining() < 18) return false; consume(18); return true;
            default: return false;
        }
    }

    void peek_and_skip_object();
    size_t consume_msgpack_header_and_get_len(uint8_t type_byte);

protected:
    seastar::net::packet& _packet_chain;
    size_t _frag_idx     = 0;
    size_t _frag_offset  = 0;
    size_t _global_offset = 0;
};

// ---------------------------------------------------------------------------
// PacketCursorFull: adds try_get_contiguous, read_split, debug_cursor, reset.
// Overrides consume() to also emit debug output.
// ---------------------------------------------------------------------------
class PacketCursorFull : public PacketCursor {
public:
    PacketCursorFull(seastar::net::packet& packet_chain)
        : PacketCursor(packet_chain) {}

    int try_get_contiguous(size_t len, std::string_view& sv) {
        if (remaining() < len) return -1;
        if (_frag_idx >= _packet_chain.nr_frags()) return -2;
        const auto& cur_frag = _packet_chain.frag(_frag_idx);
        size_t frag_rem = cur_frag.size - _frag_offset;
        if (frag_rem >= len) {
            sv = std::string_view(cur_frag.base + _frag_offset, len);
            consume(len);
            return 0;
        }
        return 1;
    }

    int read_split(char* target, size_t len) {
        if (remaining() < len) return -1;
        size_t bytes_copied = 0;
        while (bytes_copied < len && _frag_idx < _packet_chain.nr_frags()) {
            const auto& cur_frag = _packet_chain.frag(_frag_idx);
            size_t frag_rem = cur_frag.size - _frag_offset;
            size_t to_copy = std::min(len - bytes_copied, frag_rem);
            std::memcpy(target + bytes_copied, cur_frag.base + _frag_offset, to_copy);
            bytes_copied += to_copy;
            consume(to_copy);
        }
        return 0;
    }

    void debug_cursor() {
        fmt::print("chain nr frags {} len {} frag idx {} offset {} global offset {}\n",
            _packet_chain.nr_frags(), _packet_chain.len(),
            _frag_idx, _frag_offset, _global_offset);
    }

    void reset() {
        _packet_chain  = seastar::net::packet();
        _frag_idx      = 0;
        _frag_offset   = 0;
        _global_offset = 0;
    }

    // Override consume to emit debug trace during normal parsing
    void consume(size_t len) {
        PacketCursor::consume(len);
//        debug_cursor();
    }

    uint8_t  read_uint8();
    uint16_t read_uint16();
    uint32_t read_uint32();
};

// ---------------------------------------------------------------------------
// PacketCursor out-of-line definitions (need PacketCursorFull visible)
// ---------------------------------------------------------------------------
inline void PacketCursor::peek_and_skip_object() {
    uint8_t type = 0;
    if (peek_byte(type, 0)) return;
    if ((type & 0x80) == 0 || (type & 0xE0) == 0xE0) { consume(1); return; }
    if ((type & 0xE0) == 0xA0) { consume(1 + (type & 0x1F)); return; }
    if ((type & 0xF0) == 0x90) {
        size_t n = type & 0x0F; consume(1);
        for (size_t i = 0; i < n; ++i) peek_and_skip_object(); return;
    }
    if ((type & 0xF0) == 0x80) {
        size_t n = type & 0x0F; consume(1);
        for (size_t i = 0; i < n*2; ++i) peek_and_skip_object(); return;
    }
    switch (type) {
        case 0xC0: case 0xC2: case 0xC3: consume(1); return;
        case 0xCC: consume(2); return; case 0xCD: consume(3); return;
        case 0xCE: consume(5); return; case 0xCF: consume(9); return;
        case 0xD0: consume(2); return; case 0xD1: consume(3); return;
        case 0xD2: consume(5); return; case 0xD3: consume(9); return;
        case 0xCA: consume(5); return; case 0xCB: consume(9); return;
        case 0xD9: { consume(1); uint8_t l=0; peek_byte(l,0); consume(1); consume(l); return; }
        case 0xDA: { consume(3); return; }
        case 0xDB: { consume(5); return; }
        case 0xC4: { consume(1); uint8_t l=0; peek_byte(l,0); consume(1); consume(l); return; }
        case 0xC5: { consume(3); return; } case 0xC6: { consume(5); return; }
        case 0xDC: { size_t n=consume_msgpack_header_and_get_len(type); for(size_t i=0;i<n;++i) peek_and_skip_object(); return; }
        case 0xDD: { size_t n=consume_msgpack_header_and_get_len(type); for(size_t i=0;i<n;++i) peek_and_skip_object(); return; }
        case 0xDE: case 0xDF: { size_t n=consume_msgpack_header_and_get_len(type); for(size_t i=0;i<n*2;++i) peek_and_skip_object(); return; }
        case 0xD4: consume(3); return; case 0xD5: consume(4); return;
        case 0xD6: consume(6); return; case 0xD7: consume(10); return; case 0xD8: consume(18); return;
        default: throw std::runtime_error("Malformed MessagePack stream token");
    }
}

inline size_t PacketCursor::consume_msgpack_header_and_get_len(uint8_t type_byte) {
    consume(1);
    if ((type_byte & 0xE0) == 0xA0) return type_byte & 0x1F;
    if ((type_byte & 0xF0) == 0x90) return type_byte & 0x0F;
    if ((type_byte & 0xF0) == 0x80) return type_byte & 0x0F;
    auto* full = static_cast<PacketCursorFull*>(this);
    if (type_byte == 0xD9 || type_byte == 0xC4) return full->read_uint8();
    if (type_byte == 0xDA || type_byte == 0xDC || type_byte == 0xC5) return full->read_uint16();
    if (type_byte == 0xDB || type_byte == 0xDD || type_byte == 0xC6) return full->read_uint32();
    if (type_byte == 0xDE) return full->read_uint16();
    if (type_byte == 0xDF) return full->read_uint32();
    return 0;
}

inline uint8_t PacketCursorFull::read_uint8() {
    uint8_t val = 0; peek_byte(val, 0); consume(1); return val;
}
inline uint16_t PacketCursorFull::read_uint16() {
    uint16_t val = 0;
    std::string_view sv;
    auto ret = try_get_contiguous(2, sv);
    if      (ret == 0) val = *reinterpret_cast<const uint16_t*>(sv.data());
    else if (ret == 1) { char b[2]; read_split(b, 2); val = *reinterpret_cast<const uint16_t*>(b); }
    return std::byteswap(val);
}
inline uint32_t PacketCursorFull::read_uint32() {
    uint32_t val = 0;
    std::string_view sv;
    auto ret = try_get_contiguous(4, sv);
    if      (ret == 0) val = *reinterpret_cast<const uint32_t*>(sv.data());
    else if (ret == 1) { char b[4]; read_split(b, 4); val = *reinterpret_cast<const uint32_t*>(b); }
    return std::byteswap(val);
}

// ---------------------------------------------------------------------------
// ProtocolEngine
// ---------------------------------------------------------------------------
class ProtocolEngine {
public:
    ProtocolEngine() : _cursor(_packet_chain),_eventsCount(0) {}

    void append(seastar::temporary_buffer<char> buf) {
        _packet_chain.append(seastar::net::packet(std::move(buf)));
    }

    // net::packet::append() chains deleters onto every fragment ever
    // appended; already-consumed fragments are never actually freed until
    // the whole packet is destroyed. Without this, a connection that keeps
    // trickling in new bytes while only ever having a partial trailing
    // message buffered would grow memory without bound. Copies just the
    // unconsumed tail into a fresh packet so old fragments can be freed.
    void compact() {
        size_t rem = _cursor.remaining();
        if (_cursor.offset() == 0) return; // nothing consumed, nothing to reclaim
        if (rem == 0) {
            _packet_chain = seastar::net::packet();
            _cursor.rebase();
            return;
        }
        std::vector<char> tail(rem);
        _cursor.read_split(tail.data(), rem);
        _packet_chain = seastar::net::packet(seastar::temporary_buffer<char>(tail.data(), rem));
        _cursor.rebase();
    }

    uint64_t getElementCount() { return _eventsCount; }

    // Probes whether the full message at the cursor is present, using a
    // snapshot so the real cursor is not advanced.
    // Returns: 0 = complete and ready to parse, 1 = incomplete (need more
    // data), -1 = protocol violation (never becomes valid, caller should
    // close the connection rather than keep buffering it).
    int fullPacketReceived() {
	PacketCursor cursor(_cursor);

        // 1. Root array header
        uint8_t root_byte = 0;
        if (cursor.peek_byte(root_byte, 0)) return 1;
        if ((root_byte & 0xF0) != 0x90 && root_byte != 0xDC && root_byte != 0xDD) return -1;
        size_t root_array_size = cursor.consume_msgpack_header_and_get_len(root_byte);
        if (root_array_size < 2) return -1;

        // 2. Skip tag (1st element — always a string)
        uint8_t tag_byte = 0;
        if (cursor.peek_byte(tag_byte, 0)) return 1;
        size_t tag_len = cursor.consume_msgpack_header_and_get_len(tag_byte);
        if (tag_len > MAX_TAG_LEN) return -1;
        if (cursor.remaining() < tag_len) return 1;
        cursor.consume(tag_len);

        // 3. Peek 2nd element to determine format mode (do NOT consume yet)
        uint8_t format_byte = 0;
        if (cursor.peek_byte(format_byte, 0)) return 1;

        ForwardFormat mode;
        if ((format_byte & 0x80) == 0 || (format_byte & 0xE0) == 0xE0 ||
            format_byte == 0xCC || format_byte == 0xCD || format_byte == 0xCE || format_byte == 0xCF ||
            (format_byte >= 0xD4 && format_byte <= 0xD8) || (format_byte >= 0xC7 && format_byte <= 0xC9)) {
            mode = ForwardFormat::Message;
        } else if ((format_byte & 0xF0) == 0x90 || format_byte == 0xDC || format_byte == 0xDD) {
            mode = ForwardFormat::Forward;
        } else if ((format_byte & 0xE0) == 0xA0 || (format_byte >= 0xDB && format_byte <= 0xDF) ||
                   (format_byte >= 0xC4 && format_byte <= 0xC6)) {
            mode = ForwardFormat::PackedForward;
        } else {
            return -1;
        }

        if (mode == ForwardFormat::Message) {
            // Requires at least [tag, time, record]
            if (root_array_size < 3) return -1;
            // 2nd element: time (integer / ext)
            if (!cursor.try_skip_object()) return 1;
            // 3rd element: record (map)
            if (!cursor.try_skip_object()) return 1;
            // 4th element: options (map) — optional
            if (root_array_size >= 4) {
                if (!cursor.try_skip_object()) return 1;
            }
        } else if (mode == ForwardFormat::Forward) {
            // Guard: peek the declared entry count before committing to skip it.
            {
                PacketCursor probe(cursor);
                uint8_t b = 0;
                if (probe.peek_byte(b, 0)) return 1;
                size_t entries_guess = probe.consume_msgpack_header_and_get_len(b);
                if (entries_guess > MAX_ENTRIES_PER_FORWARD) return -1;
            }
            // 2nd element: entries array
            if (!cursor.try_skip_object()) return 1;
            // 3rd element: options — optional
            if (root_array_size >= 3) {
                if (!cursor.try_skip_object()) return 1;
            }
        } else { // PackedForward
            // Guard: peek the declared blob length before committing to skip it.
            {
                PacketCursor probe(cursor);
                uint8_t b = 0;
                if (probe.peek_byte(b, 0)) return 1;
                size_t blob_guess = probe.consume_msgpack_header_and_get_len(b);
                if (blob_guess > MAX_PACKED_BLOB_BYTES) return -1;
            }
            // 2nd element: binary or string blob
            if (!cursor.try_skip_object()) return 1;
            // 3rd element: options — optional
            if (root_array_size >= 3) {
                if (!cursor.try_skip_object()) return 1;
            }
        }

        return 0;
    }

    int process_incoming_packet() {
        // Guard: cap how much unparsed backlog a single connection may hold,
        // regardless of what any declared length claims (protects against
        // slow/adversarial clients that never complete a message).
        if (_cursor.remaining() > MAX_PENDING_BYTES) return -1;

        // Gate: reject parsing if the complete message is not yet in the buffer
        int gate = fullPacketReceived();
        if (gate != 0) return gate;

        // 1. Root array header
        uint8_t root_byte = 0;
        if (_cursor.peek_byte(root_byte, 0)) return 1;
        if ((root_byte & 0xF0) != 0x90 && root_byte != 0xDC && root_byte != 0xDD) return -1;
        size_t root_array_size = _cursor.consume_msgpack_header_and_get_len(root_byte);
        if (root_array_size < 2) return 1;

        // 2. Extract Tag
        uint8_t tag_byte = 0;
        if (_cursor.peek_byte(tag_byte, 0)) return 1;
        size_t tag_len = _cursor.consume_msgpack_header_and_get_len(tag_byte);
        if (tag_len > MAX_TAG_LEN) return -1;
        std::string_view tag;
        char tag_backed[MAX_TAG_LEN + 1];
        auto ret = _cursor.try_get_contiguous(tag_len, tag);
        if (ret == 1) {
            if (_cursor.read_split(tag_backed, tag_len) == -1) return 1;
            tag = std::string_view(tag_backed, tag_len);
        } else if (ret == -2) {
            return -1;
        } else if (ret == -1) {
            return 1;
        }
//        fmt::print("tag len {}\n", tag_len);
//        for (auto i = 0; i < (int)tag_len; i++) fmt::print("{}\n", tag.data()[i]);

        // 3. Determine mode
        uint8_t format_byte = 0;
        if (_cursor.peek_byte(format_byte, 0)) return 1;
        ForwardFormat mode;
        if ((format_byte & 0x80) == 0 || (format_byte & 0xE0) == 0xE0 ||
            format_byte == 0xCC || format_byte == 0xCD || format_byte == 0xCE || format_byte == 0xCF ||
            (format_byte >= 0xD4 && format_byte <= 0xD8) || (format_byte >= 0xC7 && format_byte <= 0xC9)) {
            mode = ForwardFormat::Message;
        } else if ((format_byte & 0xF0) == 0x90 || format_byte == 0xDC || format_byte == 0xDD) {
            mode = ForwardFormat::Forward;
        } else if ((format_byte & 0xE0) == 0xA0 || (format_byte >= 0xDB && format_byte <= 0xDF) ||
                   (format_byte >= 0xC4 && format_byte <= 0xC6)) {
            mode = ForwardFormat::PackedForward;
        } else {
            return -1;
        }
  //      fmt::print("{} {} {} {}\n", __FILE__, __LINE__, tag, (int)mode);

        // 4. Mode dispatch
        if (mode == ForwardFormat::Message) {
            int r = parse_single_message(tag);
            if (r != 0) return r;
        } else if (mode == ForwardFormat::Forward) {
            size_t entries_count = _cursor.consume_msgpack_header_and_get_len(format_byte);
            if (entries_count > MAX_ENTRIES_PER_FORWARD) return -1;
            TelemetrySignal signal = extract_signal_from_options(root_array_size, 3);
            int r = parse_forward_array(tag, entries_count, signal);
            if (r != 0) return r;
        } else {
            size_t binary_len = _cursor.consume_msgpack_header_and_get_len(format_byte);
            if (binary_len > MAX_PACKED_BLOB_BYTES) return -1;
            std::string_view raw_entries_blob;
            std::vector<char> raw_entries_blob_backed(binary_len);
            ret = _cursor.try_get_contiguous(binary_len, raw_entries_blob);
            if (ret == 1) {
                if (_cursor.read_split(raw_entries_blob_backed.data(), binary_len) == -1) return 1;
                raw_entries_blob = std::string_view(raw_entries_blob_backed.data(), binary_len);
            } else if (ret < 0) {
                return (ret == -2) ? -1 : 1;
            }
            std::string algo = "";
            TelemetrySignal signal = TelemetrySignal::Logs;
            inspect_packed_options(root_array_size, algo, signal);
            // Decompress if needed, then parse
	    if (algo == "gzip"){
			std::vector<char> decompressed;
			auto ret = decompress_buffer(raw_entries_blob.data(), raw_entries_blob.size(), decompressed);
			if (ret != 0){
				return -1;
			}
			parse_stream_by_signal(decompressed.data(), decompressed.size(), signal);
	    }else{
			parse_stream_by_signal(raw_entries_blob.data(), raw_entries_blob.size(), signal);
	    }
        }
        return 0;
    }

    PacketCursorFull _cursor;

private:
    int parse_single_message(std::string_view tag) {
        // Message format: [tag, time, record, options?]
        // Skip time (element 1)
        uint8_t time_type = 0;
        if (_cursor.peek_byte(time_type, 0)) return 1;
        if ((time_type & 0x80) == 0 || (time_type & 0xE0) == 0xE0 ||
            time_type == 0xCC || time_type == 0xCD || time_type == 0xCE || time_type == 0xCF) {
            size_t time_len = _cursor.consume_msgpack_header_and_get_len(time_type);
            _cursor.consume(time_len);
        } else if (time_type == 0xD7) {
            _cursor.consume(1);
            uint8_t ext_type = 0;
            if (_cursor.peek_byte(ext_type, 0)) return 1;
            _cursor.consume(1);
            _cursor.consume(8);
        } else {
            return 1;
        }

        // Record map (element 2)
        uint8_t record_map_byte = 0;
        if (_cursor.peek_byte(record_map_byte, 0)) return 1;

        PacketCursor cursor(_cursor);
        size_t map_start_offset = cursor.offset();
        size_t map_pairs_count = cursor.consume_msgpack_header_and_get_len(record_map_byte);
        calculate_msgpack_map_body_length(cursor, map_pairs_count);
        size_t map_total_bytes = cursor.offset() - map_start_offset;

        std::string_view raw_map_slice;
        auto ret2 = _cursor.try_get_contiguous(map_total_bytes, raw_map_slice);
        if (ret2 == 0) {
            StackLogFrame parsed_index = parse_msgpack_map_zero_copy(raw_map_slice);
	    _eventsCount += /*parsed_index.count*/1;
        } else if (ret2 == 1) {
            handle_split_map_record(_cursor, map_total_bytes);
        } else {
            return (ret2 == -2) ? -1 : 1;
        }

        // Options map (element 3) if present
        uint8_t opt_byte = 0;
        if (!_cursor.peek_byte(opt_byte, 0)) {
            if ((opt_byte & 0xF0) == 0x80 || opt_byte == 0xDE || opt_byte == 0xDF) {
                _cursor.peek_and_skip_object();
            }
        }
        return 0;
    }


    int parse_forward_array(std::string_view tag, size_t entries_count, TelemetrySignal sig) {
        for (size_t i = 0; i < entries_count; ++i) {
            uint8_t entry_array_byte = 0;
            if (_cursor.peek_byte(entry_array_byte, 0)) return 1;
            if ((entry_array_byte & 0xF0) != 0x90 && entry_array_byte != 0xDC && entry_array_byte != 0xDD) return -1;
            size_t sub_array_len = _cursor.consume_msgpack_header_and_get_len(entry_array_byte);
            if (sub_array_len < 2) return 1;

            // Skip timestamp
            uint8_t time_type = 0;
            if (_cursor.peek_byte(time_type, 0)) return 1;
            if ((time_type & 0x80) == 0 || (time_type & 0xE0) == 0xE0 ||
                time_type == 0xCC || time_type == 0xCD || time_type == 0xCE || time_type == 0xCF) {
                size_t time_len = _cursor.consume_msgpack_header_and_get_len(time_type);
                _cursor.consume(time_len);
            } else if (time_type == 0xD7) {
                _cursor.consume(1);
                uint8_t ext_type = 0;
                if (_cursor.peek_byte(ext_type, 0)) return 1;
                _cursor.consume(1);
                _cursor.consume(8);
            }

            // Record map
            uint8_t record_map_byte = 0;
            if (_cursor.peek_byte(record_map_byte, 0)) return 1;

            // Dry-run: snapshot → consume header + traverse pairs → measure total bytes → restore.
            // This gives us the precise byte span without side-effects on the real cursor.
            PacketCursor cursor(_cursor);
            size_t map_start_offset = cursor.offset();
            size_t map_pairs_count = cursor.consume_msgpack_header_and_get_len(record_map_byte);
            calculate_msgpack_map_body_length(cursor, map_pairs_count);   // advances cursor through body
            size_t map_total_bytes = cursor.offset() - map_start_offset;  // header + body

            // Now read the map as a single slice (guaranteed present by fullPacketReceived)
            std::string_view raw_map_slice;
            auto ret2 = _cursor.try_get_contiguous(map_total_bytes, raw_map_slice);
            if (ret2 == 0) {
                // Zero-copy: map is contiguous in current fragment
                StackLogFrame parsed_index = parse_msgpack_map_zero_copy(raw_map_slice);
		_eventsCount += /*parsed_index.count*/1;
            } else if (ret2 == 1) {
                // Map spans fragment boundaries — copy-assemble then parse
                handle_split_map_record(_cursor, map_total_bytes);
            } else {
                return (ret2 == -2) ? -1 : 1;
            }
        }
        return 0;
    }

    void inspect_packed_options(size_t total_elements, std::string& out_algo, TelemetrySignal& out_signal) {
        if (total_elements < 3) { out_algo = ""; out_signal = TelemetrySignal::Logs; return; }
        uint8_t map_byte = 0;
        if (_cursor.peek_byte(map_byte, 0)) return;
        if ((map_byte & 0xF0) != 0x80 && map_byte != 0xDE && map_byte != 0xDF) {
            out_algo = ""; out_signal = TelemetrySignal::Logs; return;
        }
        size_t map_pairs = _cursor.consume_msgpack_header_and_get_len(map_byte);
        for (size_t i = 0; i < map_pairs; ++i) {
            uint8_t key_byte = 0;
            if (_cursor.peek_byte(key_byte, 0)) return;
            size_t key_len = _cursor.consume_msgpack_header_and_get_len(key_byte);
            if (key_len > MAX_OPTION_STRING_LEN) {
                // Not a key we recognize anyway — stay in sync without copying it.
                _cursor.consume(key_len);
                _cursor.peek_and_skip_object();
                continue;
            }
            std::string_view key;
            char key_backed[MAX_OPTION_STRING_LEN + 1];
            auto ret = _cursor.try_get_contiguous(key_len, key);
            if (ret == 1) {
                if (_cursor.read_split(key_backed, key_len)) return;
                key = std::string_view(key_backed, key_len);
            } else if (ret < 0) return;

            if (key == "compressed") {
                uint8_t val_byte = 0;
                if (_cursor.peek_byte(val_byte, 0)) return;
                _cursor.consume(1);
                if (val_byte == 0xC3) {
                    out_algo = "gzip";  // true means gzip
                } else if (val_byte == 0xA4 || val_byte == 0xD9) {
                    size_t str_len = (val_byte == 0xD9) ?
                        _cursor.consume_msgpack_header_and_get_len(val_byte) : (val_byte & 0x1F);
                    if (str_len > MAX_OPTION_STRING_LEN) {
                        _cursor.consume(str_len);
                        out_algo = "";
                        continue;
                    }
                    std::string_view algo;
                    char algo_backed[MAX_OPTION_STRING_LEN + 1];
                    ret = _cursor.try_get_contiguous(str_len, algo);
                    if (ret == 1) {
                        if (_cursor.read_split(algo_backed, str_len)) return;
                        algo = std::string_view(algo_backed, str_len);
                    } else if (ret < 0) return;
                    out_algo = std::string(algo);
                } else {
                    out_algo = "";
                }
            } else if (key == "fluent_signal") {
                uint8_t val_byte = 0;
                if (_cursor.peek_byte(val_byte, 0)) return;
                if ((val_byte & 0x80) == 0) {
                    out_signal = static_cast<TelemetrySignal>(val_byte & 0x7F);
                    _cursor.consume(1);
                } else if (val_byte == 0xCC) {
                    _cursor.consume(1);
                    uint8_t tmp = 0;
                    if (_cursor.peek_byte(tmp, 0)) return;
                    out_signal = static_cast<TelemetrySignal>(tmp);
                    _cursor.consume(1);
                } else {
                    out_signal = TelemetrySignal::Logs;
                    _cursor.peek_and_skip_object();
                }
            } else {
                _cursor.peek_and_skip_object();
            }
        }
    }

    size_t calculate_msgpack_map_body_length(PacketCursor& cursor, size_t pairs_count) {
        size_t initial_offset = cursor.offset();
        for (size_t i = 0; i < pairs_count; ++i) {
            cursor.peek_and_skip_object();
            cursor.peek_and_skip_object();
        }
        size_t final_offset = cursor.offset();
//        fmt::print("{} {} {}\n", __func__, __LINE__, final_offset - initial_offset);
        return final_offset - initial_offset;
    }

    void handle_split_map_record(PacketCursorFull& cursor, size_t total_bytes) {
        // Map spans fragment boundaries — assemble into a contiguous heap buffer,
        // then hand off to the same zero-copy parser used for single-fragment maps.
        // fullPacketReceived() guarantees total_bytes are present in the chain.
        std::vector<char> buf(total_bytes);
        if (cursor.read_split(buf.data(), total_bytes) != 0) {
            fmt::print("[WARN] handle_split_map_record: read_split failed for {} bytes\n", total_bytes);
            return;
        }
        StackLogFrame parsed_index = parse_msgpack_map_zero_copy(
            std::string_view(buf.data(), total_bytes));
	_eventsCount += /*parsed_index.count*/1;
//        fmt::print("[DEBUG] handle_split_map_record: parsed {} fields from {}-byte split map\n",
//            parsed_index.count, total_bytes);
        // TODO: dispatch parsed_index to sink
    }

    int decompress_buffer(const char* data, size_t len, std::vector<char>& decompressedData) {
        // TODO: Integrate zlib for actual gzip decompression
//        fmt::print("[DEBUG] gzip decompression placeholder, len={}\n", len);
	z_stream zs;
	memset(&zs, 0, sizeof(zs));

	// Initialize zlib for gzip decompression (MAX_WBITS + 16 enables gzip decoding)
	if (inflateInit2(&zs, MAX_WBITS + 16) != Z_OK) {
		fmt::print("Failed to initialize zlib for inflate.\n");
		return -1;
	}
	zs.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(data));
	zs.avail_in = len;

	char outBuffer[32768]; // 32KB chunk
	int ret;
	// Decompress chunk by chunk
	do {
		zs.next_out = reinterpret_cast<Bytef*>(outBuffer);
		zs.avail_out = sizeof(outBuffer);

		ret = inflate(&zs, Z_NO_FLUSH);

		if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
			inflateEnd(&zs);
			fmt::print("Decompression error during inflate.\n");
			return -1;
		}

		// Append the chunk to our vector
		size_t bytesDecompressed = sizeof(outBuffer) - zs.avail_out;
		if (bytesDecompressed > 0) {
			if (decompressedData.size() + bytesDecompressed > MAX_DECOMPRESSED_BYTES) {
				inflateEnd(&zs);
				fmt::print("Decompression aborted: exceeded max decompressed size ({} bytes)\n", MAX_DECOMPRESSED_BYTES);
				return -1;
			}
			decompressedData.insert(decompressedData.end(), outBuffer, outBuffer + bytesDecompressed);
		}
	} while (ret == Z_OK);

	inflateEnd(&zs);
	if (ret != Z_STREAM_END) {
		fmt::print("Decompression failed: incomplete stream.\n");
		return -1;
	}
        return 0;
    }

    void parse_stream_by_signal(const char* data, size_t len, TelemetrySignal sig) {
        // Parse a raw binary stream (typically from PackedForward)
        // Stream contains entries: each entry is [timestamp, record_map]
        
        if (len == 0){
		fmt::print("{} {}\n",__func__,__LINE__);
		return;
	}
        
        // Create a temporary packet from raw data for cursor operations
        auto temp_buf = seastar::temporary_buffer<char>(data, len);
        seastar::net::packet temp_pkt(std::move(temp_buf));
        PacketCursorFull temp_cursor(temp_pkt);
        
        // Parse entries from the stream
        while (temp_cursor.remaining() > 0) {
            // Each entry is a [timestamp, record] array
            uint8_t entry_byte = 0;
            if (temp_cursor.peek_byte(entry_byte, 0)) break;
            
            if ((entry_byte & 0xF0) != 0x90 && entry_byte != 0xDC && entry_byte != 0xDD) {
                break;
            }
            
            size_t entry_size = temp_cursor.consume_msgpack_header_and_get_len(entry_byte);
            if (entry_size < 2) break;
            
            // Skip timestamp
            uint8_t time_byte = 0;
            if (temp_cursor.peek_byte(time_byte, 0)) break;
            if ((time_byte & 0x80) == 0 || (time_byte & 0xE0) == 0xE0 ||
                time_byte == 0xCC || time_byte == 0xCD || time_byte == 0xCE || time_byte == 0xCF) {
                size_t time_len = temp_cursor.consume_msgpack_header_and_get_len(time_byte);
                temp_cursor.consume(time_len);
            } else if (time_byte == 0xD7) {
                temp_cursor.consume(1);
                uint8_t ext_type = 0;
                if (temp_cursor.peek_byte(ext_type, 0)) break;
                temp_cursor.consume(1);
                temp_cursor.consume(8);
            } else {
                break;
            }
            
            // Extract record map
            uint8_t record_byte = 0;
            if (temp_cursor.peek_byte(record_byte, 0)) break;
            
            PacketCursor cursor_probe(temp_cursor);
            size_t map_start = cursor_probe.offset();
            size_t map_pairs = cursor_probe.consume_msgpack_header_and_get_len(record_byte);
            calculate_msgpack_map_body_length(cursor_probe, map_pairs);
            size_t map_bytes = cursor_probe.offset() - map_start;
            
            std::string_view map_data;
            auto ret = temp_cursor.try_get_contiguous(map_bytes, map_data);
            if (ret == 0) {
                parse_msgpack_map_zero_copy(map_data);
            } else if (ret == 1) {
                temp_cursor.peek_and_skip_object();
            } else {
                break;
            }
        }
    }

    TelemetrySignal extract_signal_from_options(size_t total_elements, size_t target_idx) {
        // Navigate to options map and extract "fluent_signal" key without advancing main cursor.
        // Uses a temporary copy-constructed PacketCursor for non-destructive probing.
        if (total_elements <= target_idx) {
            return TelemetrySignal::Logs;
        }
        
        PacketCursorFull cursor(_cursor);
        
        // Skip tag (element 0)
        if (!cursor.try_skip_object()) {
            return TelemetrySignal::Logs;
        }
        
        // Skip 2nd element (time/entries/blob)
        if (!cursor.try_skip_object()) {
            return TelemetrySignal::Logs;
        }
        
        // Now at options map
        uint8_t map_byte = 0;
        if (cursor.peek_byte(map_byte, 0)) {
            return TelemetrySignal::Logs;
        }
        
        // Check it's a map
        if ((map_byte & 0xF0) != 0x80 && map_byte != 0xDE && map_byte != 0xDF) {
            return TelemetrySignal::Logs;
        }
        
        size_t map_pairs = cursor.consume_msgpack_header_and_get_len(map_byte);
        TelemetrySignal result = TelemetrySignal::Logs;
        
        for (size_t i = 0; i < map_pairs; ++i) {
            uint8_t key_byte = 0;
            if (cursor.peek_byte(key_byte, 0)) break;
            size_t key_len = cursor.consume_msgpack_header_and_get_len(key_byte);
            
            std::string_view key;
            char key_backed[key_len + 1];
            auto ret = cursor.try_get_contiguous(key_len, key);
            if (ret == 1) {
                if (cursor.read_split(key_backed, key_len)) break;
                key = std::string_view(key_backed, key_len);
            } else if (ret < 0) {
                break;
            }
            
            if (key == "fluent_signal") {
                uint8_t val_byte = 0;
                if (cursor.peek_byte(val_byte, 0)) break;
                if ((val_byte & 0x80) == 0) {
                    result = static_cast<TelemetrySignal>(val_byte & 0x7F);
                    cursor.consume(1);
                } else if (val_byte == 0xCC) {
                    cursor.consume(1);
                    uint8_t tmp = 0;
                    if (cursor.peek_byte(tmp, 0)) break;
                    result = static_cast<TelemetrySignal>(tmp);
                    cursor.consume(1);
                } else {
                    cursor.peek_and_skip_object();
                }
                break;
            } else {
                cursor.peek_and_skip_object();
            }
        }
        
        return result;
    }

    seastar::net::packet _packet_chain;
    uint64_t _eventsCount;
};

}
