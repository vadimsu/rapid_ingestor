#pragma once
#include <cstdint>
#include <optional>
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

// RapidIngestor::MsgPackElement::as_int() only ever sees the 4-byte branch
// coming out of parse_msgpack_map_zero_copy() (the only integer encoding it
// recognizes is 0xD2/int32), decoded as a byteswapped int32_t - so int64
// template fields are written as msgpack int32 (big-endian, two's
// complement), not a true 8-byte int64.
inline void write_int32(std::vector<char>& buf, int32_t v) {
    buf.push_back(char(0xD2));
    put_u32be(buf, static_cast<uint32_t>(v));
}

inline void write_bool(std::vector<char>& buf, bool v) {
    buf.push_back(char(v ? 0xC3 : 0xC2));
}

// One field in a generated record. Mirrors the shape of RapidIngestor's
// ClickHouse sink `Sink::columns` entries (name/type) plus generator-only
// knobs that control what value gets rotated in per log entry - see
// load_template() in forward_template_loader.hh for the JSON schema this
// is built from.
struct FieldSpec {
    enum class Type { String, Int64, Bool, Map };

    std::string name;
    Type type = Type::String;

    // String: `values` (if non-empty) rotates by entry index; otherwise a
    // fixed `size`-byte dummy payload is used (clamped to 31, fixstr-only).
    std::vector<std::string> values;
    size_t size = 16;

    // Int64: rotates min..max by entry index.
    int64_t min = 0;
    int64_t max = 1000;

    // Bool: fixed value if set, otherwise alternates by entry index.
    std::optional<bool> fixed_bool;

    // Map: nested fields (fixmap of fixstr keys -> recursively-typed values).
    std::vector<FieldSpec> fields;
};

inline void write_field_value(std::vector<char>& buf, const FieldSpec& spec, size_t entry_index) {
    switch (spec.type) {
        case FieldSpec::Type::String: {
            if (!spec.values.empty()) {
                write_fixstr(buf, spec.values[entry_index % spec.values.size()]);
            } else {
                write_fixstr(buf, std::string(std::min<size_t>(spec.size, 31), 'x'));
            }
            break;
        }
        case FieldSpec::Type::Int64: {
            int64_t range = spec.max - spec.min + 1;
            int64_t v = spec.min;
            if (range > 0) v += static_cast<int64_t>(entry_index % static_cast<uint64_t>(range));
            write_int32(buf, static_cast<int32_t>(v));
            break;
        }
        case FieldSpec::Type::Bool: {
            bool v = spec.fixed_bool.value_or(entry_index % 2 == 0);
            write_bool(buf, v);
            break;
        }
        case FieldSpec::Type::Map: {
            write_map_header(buf, spec.fields.size());
            for (auto& f : spec.fields) {
                write_fixstr(buf, f.name);
                write_field_value(buf, f, entry_index);
            }
            break;
        }
    }
}

// Generic "fieldN" string fields (the historical --fields/--field-size
// behavior, used whenever no --template is given).
inline std::vector<FieldSpec> make_generic_fields(size_t fields, size_t field_value_size) {
    std::vector<FieldSpec> specs;
    specs.reserve(fields);
    for (size_t f = 0; f < fields; ++f) {
        FieldSpec spec;
        spec.name = "field" + std::to_string(f);
        spec.type = FieldSpec::Type::String;
        spec.size = field_value_size;
        specs.push_back(std::move(spec));
    }
    return specs;
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

// Builds a Forward-mode packet from an explicit field schema (see FieldSpec),
// e.g. one loaded from a --template JSON file so generated records match a
// specific ClickHouse sink schema variant (single-column, multi-column,
// multi-column with aliasing, ...).
inline MessageTemplate build_forward_template(std::string_view tag, size_t entries,
                                               const std::vector<FieldSpec>& fields) {
    MessageTemplate tmpl;
    tmpl.entries = entries;
    auto& buf = tmpl.bytes;
    buf.reserve(32 + entries * (16 + fields.size() * 32));

    write_array_header(buf, 2); // [tag, entries]
    write_str(buf, tag);
    write_array_header(buf, entries);

    for (size_t e = 0; e < entries; ++e) {
        write_array_header(buf, 2); // [time, record]
        if (e == 0) tmpl.time_offset = buf.size() + 2; // skip 0xD7 + ext-type byte
        write_event_time(buf, 0, 0);
        write_map_header(buf, fields.size());
        for (auto& f : fields) {
            write_fixstr(buf, f.name);
            write_field_value(buf, f, e);
        }
    }
    return tmpl;
}

// Legacy entry point: generic "fieldN" string fields, kept for the plain
// --fields/--field-size CLI options (no --template given).
inline MessageTemplate build_forward_template(std::string_view tag, size_t entries,
                                               size_t fields, size_t field_value_size) {
    return build_forward_template(tag, entries, make_generic_fields(fields, field_value_size));
}

} // namespace ForwardTrafficGen
