#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <chrono>

// Minimal MessagePack / Fluent-Forward encoder for traffic generation.
//
// Encoding choices here are deliberately constrained to byte layouts that
// RapidIngestor::ProtocolEngine (src/rapid_ingestor_message_parser.hh) is
// known to decode correctly:
//  - timestamps use the Fluentd "EventTime" ext format (0xD7, type 0), the
//    only multi-byte timestamp encoding consume_msgpack_header_and_get_len()
//    handles without losing bytes (0xCC/0xCD/0xCE/0xCF would under-consume).
//  - record map keys/values use fixstr only (len 0-31), matching
//    parse_msgpack_map_zero_copy()'s fixstr-only field decoding.
namespace ForwardTrafficGen {

inline void put_u16be(std::vector<char>& buf, uint16_t v) {
    buf.push_back(char((v >> 8) & 0xFF));
    buf.push_back(char(v & 0xFF));
}

inline void put_u32be(std::vector<char>& buf, uint32_t v) {
    buf.push_back(char((v >> 24) & 0xFF));
    buf.push_back(char((v >> 16) & 0xFF));
    buf.push_back(char((v >> 8) & 0xFF));
    buf.push_back(char(v & 0xFF));
}

inline void write_array_header(std::vector<char>& buf, size_t count) {
    if (count <= 15) {
        buf.push_back(char(0x90 | count));
    } else {
        buf.push_back(char(0xDC));
        put_u16be(buf, static_cast<uint16_t>(count));
    }
}

inline void write_map_header(std::vector<char>& buf, size_t count) {
    if (count <= 15) {
        buf.push_back(char(0x80 | count));
    } else {
        buf.push_back(char(0xDE));
        put_u16be(buf, static_cast<uint16_t>(count));
    }
}

inline void write_fixstr(std::vector<char>& buf, std::string_view s) {
    size_t len = std::min<size_t>(s.size(), 31);
    buf.push_back(char(0xA0 | len));
    buf.insert(buf.end(), s.data(), s.data() + len);
}

// Safe for the root-level tag: its header goes through
// consume_msgpack_header_and_get_len(), which decodes str8/str16 correctly.
inline void write_str(std::vector<char>& buf, std::string_view s) {
    if (s.size() <= 31) { write_fixstr(buf, s); return; }
    if (s.size() <= 0xFF) {
        buf.push_back(char(0xD9));
        buf.push_back(char(s.size()));
    } else {
        buf.push_back(char(0xDA));
        put_u16be(buf, static_cast<uint16_t>(s.size()));
    }
    buf.insert(buf.end(), s.data(), s.data() + s.size());
}

inline void write_event_time(std::vector<char>& buf, uint32_t sec, uint32_t nsec) {
    buf.push_back(char(0xD7));
    buf.push_back(char(0x00));
    put_u32be(buf, sec);
    put_u32be(buf, nsec);
}

// A pre-built Forward-mode packet: [tag, [[time, {k:v,...}], ...]].
// The wall-clock time of the first entry is patched in place before each
// send so the payload never needs to be re-encoded from scratch.
struct MessageTemplate {
    std::vector<char> bytes;
    size_t time_offset = 0;
    size_t entries = 0;

    void refresh_time() {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto sec = static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::seconds>(now).count());
        auto nsec = static_cast<uint32_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count() % 1000000000LL);
        bytes[time_offset + 0] = char((sec >> 24) & 0xFF);
        bytes[time_offset + 1] = char((sec >> 16) & 0xFF);
        bytes[time_offset + 2] = char((sec >> 8) & 0xFF);
        bytes[time_offset + 3] = char(sec & 0xFF);
        bytes[time_offset + 4] = char((nsec >> 24) & 0xFF);
        bytes[time_offset + 5] = char((nsec >> 16) & 0xFF);
        bytes[time_offset + 6] = char((nsec >> 8) & 0xFF);
        bytes[time_offset + 7] = char(nsec & 0xFF);
    }
};

inline MessageTemplate build_forward_template(std::string_view tag, size_t entries,
                                               size_t fields, size_t field_value_size) {
    MessageTemplate tmpl;
    tmpl.entries = entries;
    auto& buf = tmpl.bytes;
    buf.reserve(32 + entries * (16 + fields * (16 + field_value_size)));

    write_array_header(buf, 2); // [tag, entries]
    write_str(buf, tag);
    write_array_header(buf, entries);

    std::string value(field_value_size, 'x');
    for (size_t e = 0; e < entries; ++e) {
        write_array_header(buf, 2); // [time, record]
        if (e == 0) tmpl.time_offset = buf.size() + 2; // skip 0xD7 + ext-type byte
        write_event_time(buf, 0, 0);
        write_map_header(buf, fields);
        for (size_t f = 0; f < fields; ++f) {
            char key[16];
            int klen = std::snprintf(key, sizeof(key), "field%zu", f);
            write_fixstr(buf, std::string_view(key, static_cast<size_t>(klen)));
            write_fixstr(buf, value);
        }
    }
    return tmpl;
}

} // namespace ForwardTrafficGen
