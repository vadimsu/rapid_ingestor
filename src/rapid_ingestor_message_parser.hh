#pragma once
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>
#include <string_view>
#include <iostream>
#include <optional>
#include <bit>

namespace RapidIngestor {
enum class TelemetrySignal : uint8_t { Logs = 0, Metrics = 1, Traces = 2 };
enum class ForwardFormat     : uint8_t { Message, Forward, PackedForward };
constexpr size_t MAX_FIELDS_PER_LOG = 32;

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

    Snapshot save_state()  const { return {_frag_idx, _frag_offset, _global_offset}; }
    void restore_state(const Snapshot& s) {
        _frag_idx      = s.frag_idx;
        _frag_offset   = s.frag_offset;
        _global_offset = s.global_offset;
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
        debug_cursor();
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
    ProtocolEngine() : _cursor(_packet_chain) {}

    void append(seastar::temporary_buffer<char> buf) {
        _packet_chain.append(seastar::net::packet(std::move(buf)));
    }

    // Returns true if the entire message described by the root array is present
    // in the buffer. Uses a snapshot so the cursor is not advanced.
    bool fullPacketReceived() {
	PacketCursor cursor(_cursor);

        // 1. Root array header
        uint8_t root_byte = 0;
        if (cursor.peek_byte(root_byte, 0)) return false;
        if ((root_byte & 0xF0) != 0x90 && root_byte != 0xDC && root_byte != 0xDD) return false;
        size_t root_array_size = cursor.consume_msgpack_header_and_get_len(root_byte);
        if (root_array_size < 2) return false;

        // 2. Skip tag (1st element — always a string)
        uint8_t tag_byte = 0;
        if (cursor.peek_byte(tag_byte, 0)) return false;
        size_t tag_len = cursor.consume_msgpack_header_and_get_len(tag_byte);
        if (cursor.remaining() < tag_len) return false;
        cursor.consume(tag_len);

        // 3. Peek 2nd element to determine format mode (do NOT consume yet)
        uint8_t format_byte = 0;
        if (cursor.peek_byte(format_byte, 0)) return false;

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
            return false;
        }

        if (mode == ForwardFormat::Message) {
            // Requires at least [tag, time, record]
            if (root_array_size < 3) return false;
            // 2nd element: time (integer / ext)
            if (!cursor.try_skip_object()) return false;
            // 3rd element: record (map)
            if (!cursor.try_skip_object()) return false;
            // 4th element: options (map) — optional
            if (root_array_size >= 4) {
                if (!cursor.try_skip_object()) return false;
            }
        } else if (mode == ForwardFormat::Forward) {
            // 2nd element: entries array
            if (!cursor.try_skip_object()) return false;
            // 3rd element: options — optional
            if (root_array_size >= 3) {
                if (!cursor.try_skip_object()) return false;
            }
        } else { // PackedForward
            // 2nd element: binary or string blob
            if (!cursor.try_skip_object()) return false;
            // 3rd element: options — optional
            if (root_array_size >= 3) {
                if (!cursor.try_skip_object()) return false;
            }
        }

        return true;
    }

    int process_incoming_packet() {
        // Gate: reject parsing if the complete message is not yet in the buffer
        if (!fullPacketReceived()) return 1;

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
        std::string_view tag;
        char tag_backed[tag_len + 1];
        fmt::print("tag_len {}\n", tag_len);
        auto ret = _cursor.try_get_contiguous(tag_len, tag);
        if (ret == 1) {
            if (_cursor.read_split(tag_backed, tag_len) == -1) return 1;
            tag = std::string_view(tag_backed, tag_len);
        } else if (ret == -2) {
            return -1;
        } else if (ret == -1) {
            return 1;
        }
        fmt::print("tag len {}\n", tag_len);
        for (auto i = 0; i < (int)tag_len; i++) fmt::print("{}\n", tag.data()[i]);

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
        fmt::print("{} {} {} {}\n", __FILE__, __LINE__, tag, (int)mode);

        // 4. Mode dispatch
        if (mode == ForwardFormat::Message) {
            int r = parse_single_message(tag);
            if (r != 0) return r;
        } else if (mode == ForwardFormat::Forward) {
            size_t entries_count = _cursor.consume_msgpack_header_and_get_len(format_byte);
            TelemetrySignal signal = extract_signal_from_options(root_array_size, 3);
            int r = parse_forward_array(tag, entries_count, signal);
            if (r != 0) return r;
        } else {
            size_t binary_len = _cursor.consume_msgpack_header_and_get_len(format_byte);
            std::string_view raw_entries_blob;
            char raw_entries_blob_backed[binary_len + 1];
            ret = _cursor.try_get_contiguous(binary_len, raw_entries_blob);
            if (ret == 1) {
                if (_cursor.read_split(raw_entries_blob_backed, binary_len) == -1) return 1;
                raw_entries_blob = std::string_view(raw_entries_blob_backed, binary_len);
            } else if (ret < 0) {
                return (ret == -2) ? -1 : 1;
            }
            bool compressed = false;
            TelemetrySignal signal = TelemetrySignal::Logs;
            inspect_packed_options(root_array_size, compressed, signal);
            if (!compressed)
                parse_stream_by_signal(raw_entries_blob.data(), raw_entries_blob.size(), signal);
        }
        return 0;
    }

    PacketCursorFull _cursor;

private:
    int parse_single_message(std::string_view tag) {
        fmt::print("{} {}\n", __FILE__, __LINE__);
        return 0;
    }

    int parse_forward_array(std::string_view tag, size_t entries_count, TelemetrySignal sig) {
        for (size_t i = 0; i < entries_count; ++i) {
            uint8_t entry_array_byte = 0;
            if (_cursor.peek_byte(entry_array_byte, 0)) return 1;
            if ((entry_array_byte & 0xF0) != 0x90 && entry_array_byte != 0xDC) return -1;
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
            size_t map_start_offset = _cursor.offset();
            size_t map_pairs_count = _cursor.consume_msgpack_header_and_get_len(record_map_byte);
            size_t map_payload_len = calculate_msgpack_map_body_length(_cursor, map_pairs_count);
            size_t map_total_bytes = (_cursor.offset() - map_start_offset) + map_payload_len;

            std::string_view raw_map_slice;
            char raw_map_slice_backed[map_total_bytes];
            auto ret2 = _cursor.try_get_contiguous(map_total_bytes, raw_map_slice);
            if (ret2 == 1) {
                if (_cursor.read_split(raw_map_slice_backed, map_total_bytes)) return 1;
                raw_map_slice = std::string_view(raw_map_slice_backed, map_total_bytes);
            } else if (ret2 == -2) {
                return 1;
            }

            if (!raw_map_slice.empty()) {
                StackLogFrame parsed_index = parse_msgpack_map_zero_copy(raw_map_slice);
            } else {
                handle_split_map_record(_cursor, map_total_bytes);
            }
        }
        return 0;
    }

    void inspect_packed_options(size_t total_elements, bool& out_compressed, TelemetrySignal& out_signal) {
        if (total_elements < 3) { out_compressed = false; out_signal = TelemetrySignal::Logs; return; }
        uint8_t map_byte = 0;
        if (_cursor.peek_byte(map_byte, 0)) return;
        if ((map_byte & 0xF0) != 0x80 && map_byte != 0xDE && map_byte != 0xDF) {
            out_compressed = false; out_signal = TelemetrySignal::Logs; return;
        }
        size_t map_pairs = _cursor.consume_msgpack_header_and_get_len(map_byte);
        for (size_t i = 0; i < map_pairs; ++i) {
            uint8_t key_byte = 0;
            if (_cursor.peek_byte(key_byte, 0)) return;
            size_t key_len = _cursor.consume_msgpack_header_and_get_len(key_byte);
            std::string_view key;
            char key_backed[key_len + 1];
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
                    out_compressed = true;
                } else if (val_byte == 0xA4 || val_byte == 0xD9) {
                    size_t str_len = (val_byte == 0xD9) ?
                        _cursor.consume_msgpack_header_and_get_len(val_byte) : (val_byte & 0x1F);
                    std::string_view algo;
                    char algo_backed[str_len + 1];
                    ret = _cursor.try_get_contiguous(str_len, algo);
                    if (ret == 1) {
                        if (_cursor.read_split(algo_backed, str_len)) return;
                        algo = std::string_view(algo_backed, str_len);
                    } else if (ret < 0) return;
                    if (algo == "gzip") out_compressed = true;
                } else {
                    out_compressed = false;
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

    size_t calculate_msgpack_map_body_length(PacketCursorFull& cursor, size_t pairs_count) {
        size_t initial_offset = cursor.offset();
        for (size_t i = 0; i < pairs_count; ++i) {
            cursor.peek_and_skip_object();
            cursor.peek_and_skip_object();
        }
        size_t final_offset = cursor.offset();
        fmt::print("{} {} {}\n", __func__, __LINE__, final_offset - initial_offset);
        return final_offset - initial_offset;
    }

    void handle_split_map_record(PacketCursorFull& cursor, size_t total_bytes) {
        fmt::print("{} {}\n", __func__, __LINE__);
    }

    void parse_stream_by_signal(const char* data, size_t len, TelemetrySignal sig) {
        fmt::print("{} {}\n", __func__, __LINE__);
    }

    TelemetrySignal extract_signal_from_options(size_t total_elements, size_t target_idx) {
        return TelemetrySignal::Logs;
    }

    seastar::net::packet _packet_chain;
};

}
