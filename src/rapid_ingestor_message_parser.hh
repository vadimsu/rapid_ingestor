#pragma once
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>
#include <string_view>
#include <iostream>
#include <optional>
#include <bit>

namespace RapidIngestor {
// Internal Telemetry Signal Routing Targets
enum class TelemetrySignal : uint8_t { Logs = 0, Metrics = 1, Traces = 2 };
enum class ForwardFormat     : uint8_t { Message, Forward, PackedForward };

// Maximum fields we expect per log line to keep it on the stack
constexpr size_t MAX_FIELDS_PER_LOG = 32;

enum class MsgPackType : uint8_t {
    Integer,
    String,
    Map,
    Array,
    Boolean
};

// 16-byte lightweight reference structure
struct MsgPackElement {
    MsgPackType type;
    std::string_view key;
    std::string_view val; // Points to raw big-endian bytes or string data
    
    // In-place big-endian integer decoder
    int64_t as_int() const {
        if (val.size() == 1) return static_cast<int8_t>(val[0]);
        if (val.size() == 2) return std::byteswap(*reinterpret_cast<const int16_t*>(val.data()));
        if (val.size() == 4) return std::byteswap(*reinterpret_cast<const int32_t*>(val.data()));
        return std::byteswap(*reinterpret_cast<const int64_t*>(val.data()));
    }
};

// Fixed-capacity tracker entirely on the stack
struct StackLogFrame {
    std::array<MsgPackElement, MAX_FIELDS_PER_LOG> elements;
    size_t count = 0;

    std::optional<MsgPackElement> find(std::string_view key) const {
        for (size_t i = 0; i < count; ++i) {
            if (elements[i].key == key) return elements[i];
        }
        return std::nullopt;
    }
};

// Zero-copy traversal of the network buffer
StackLogFrame parse_msgpack_map_zero_copy(std::string_view raw_buffer) {
    StackLogFrame frame;
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(raw_buffer.data());
    const uint8_t* end = ptr + raw_buffer.size();

    if (ptr >= end || (*ptr & 0xF0) != 0x80) return frame; // Expect FixMap (0x80 - 0x8F)
    size_t map_size = *ptr & 0x0F;
    ptr++;

    for (size_t i = 0; i < map_size && ptr < end && frame.count < MAX_FIELDS_PER_LOG; ++i) {
        // 1. Parse Key (Assuming FixStr for brevity)
        if ((*ptr & 0xE0) != 0xA0) break; 
        size_t key_len = *ptr & 0x1F;
        ptr++;
        std::string_view key(reinterpret_cast<const char*>(ptr), key_len);
        ptr += key_len;

        // 2. Map Value References without allocating
        MsgPackElement elem;
        elem.key = key;

        if ((*ptr & 0xE0) == 0xA0) { // String
            elem.type = MsgPackType::String;
            size_t str_len = *ptr & 0x1F;
            ptr++;
            elem.val = std::string_view(reinterpret_cast<const char*>(ptr), str_len);
            ptr += str_len;
        } else if (*ptr == 0xD2) { // 32-bit Signed Int
            elem.type = MsgPackType::Integer;
            ptr++;
            elem.val = std::string_view(reinterpret_cast<const char*>(ptr), 4);
            ptr += 4;
        } else if ((*ptr & 0xF0) == 0x80) { // Nested Map
            elem.type = MsgPackType::Map;
            const uint8_t* start_map = ptr;
            size_t inner_map_size = *ptr & 0x0F;
            ptr++;
            // Rapidly skip inner map headers to find bounds
            for(size_t j=0; j < inner_map_size * 2; ++j) {
                if((*ptr & 0xE0) == 0xA0) ptr += ((*ptr & 0x1F) + 1);
                else ptr++; // Simplistic jump for brevity
            }
            elem.val = std::string_view(reinterpret_cast<const char*>(start_map), ptr - start_map);
        }

        frame.elements[frame.count++] = elem;
    }
    return frame;
}

// Simplified architecture matching the PacketCursor described previously
class PacketCursor {
public:
	PacketCursor() {}
	void append(seastar::temporary_buffer<char> buf) {
		// Appends to the virtual chain zero-copy (ref-counted internally)
		_packet_chain.append(seastar::net::packet(std::move(buf)));
	}
	size_t remaining() const{
		return _packet_chain.len() - _global_offset;
	}
	size_t offset() const{
		return _global_offset;
	}
	int peek_byte(uint8_t& dst, size_t ahead = 0) const{
		if  (_packet_chain.len() == 0){
			return 1;
		}
		if (_frag_idx >= _packet_chain.nr_frags()){
			return 1;
		}
		const auto& frag = _packet_chain.frag(_frag_idx);
//		const auto frag = frags[0];
		if (frag.size < 1){
			return 1;
		}
#if 0
		dst = frag.base[0];
		return 0;
#else
		// Linearized access inside seastar::net::packet via index routines
		// If the packet is heavily fragmented, packet_chain.get_header() or custom iterator is used.
		dst = reinterpret_cast<const uint8_t*>(frag.base)[_frag_offset + ahead];
		return 0;
#endif
	}
	// Fast-path lookup: returns a direct string_view if fully contained in current fragment
	int try_get_contiguous(size_t len, std::string_view& sv) {
		if (remaining() < len) return -1;//wait for more bytes
        	
		if (_frag_idx >= _packet_chain.nr_frags()) return -2;//fatal
		const auto& cur_frag = _packet_chain.frag(_frag_idx);

        	size_t frag_rem = cur_frag.size - _frag_offset;

		if (frag_rem >= len) {
			sv = std::string_view(cur_frag.base + _frag_offset, len);
			consume(len);
			return 0;//got it
		}
	        return 1; // Spans across boundaries
	}
	// Slow-path fallback: stitches split data into a stack-allocated target buffer
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
	void debug_cursor(){
		fmt::print("chain nr frags {} len {} frag idx {} offset {} global offset {}\n",_packet_chain.nr_frags(),_packet_chain.len(),_frag_idx,_frag_offset,_global_offset);
	}
	// Instantly drops reference counters to free Seastar memory allocation pools
	void reset() {
 		_packet_chain = seastar::net::packet(); // Assigns an empty packet
 		_frag_idx = 0;
  		_frag_offset = 0;
 		_global_offset = 0;
	}
	void consume(size_t len){
		_global_offset += len;
		_frag_offset += len;
        
		while (_frag_idx < _packet_chain.nr_frags() && _frag_offset >= _packet_chain.frag(_frag_idx).size) {
			_frag_offset -= _packet_chain.frag(_frag_idx).size;
			_frag_idx++;
		}
		debug_cursor();
	}

	// Recursively steps past any arbitrary MsgPack item to find its boundary
	void peek_and_skip_object() {
        	uint8_t type = 0;
		if (peek_byte(type, 0)){
			return;
		}

		// 1. Positive FixInt (0x00 - 0x7F) & Negative FixInt (0xE0 - 0xFF)
		if ((type & 0x80) == 0 || (type & 0xE0) == 0xE0) {
			consume(1);
			return;
		}
		// 2. FixStr (0xA0 - 0xBF)
		if ((type & 0xE0) == 0xA0) {
			size_t len = type & 0x1F;
			consume(1 + len);
			return;
		}
		// 3. FixArray (0x90 - 0x9F)
		if ((type & 0xF0) == 0x90) {
			size_t elements = type & 0x0F;
			consume(1);
			for (size_t i = 0; i < elements; ++i) peek_and_skip_object();
				return;
		}
		// 4. FixMap (0x80 - 0x8F)
		if ((type & 0xF0) == 0x80) {
			size_t pairs = type & 0x0F;
			consume(1);
			for (size_t i = 0; i < pairs * 2; ++i) peek_and_skip_object();
				return;
		}

		// 5. Fixed primitives / Explicit sized types
		switch (type) {
			case 0xC0: // nil
			case 0xC2: // false
			case 0xC3: // true
				consume(1); return;
			case 0xCC: consume(2); return; // uint 8
			case 0xCD: consume(3); return; // uint 16
			case 0xCE: consume(5); return; // uint 32
			case 0xCF: consume(9); return; // uint 64
			case 0xD0: consume(2); return; // int 8
			case 0xD1: consume(3); return; // int 16
			case 0xD2: consume(5); return; // int 32
			case 0xD3: consume(9); return; // int 64
			case 0xCA: consume(5); return; // float 32
			case 0xCB: consume(9); return; // float 64

			// Variable strings and binary data
			case 0xD9: { consume(1); size_t l = read_uint8();  consume(l); return; }
			case 0xDA: { consume(1); size_t l = read_uint16(); consume(l); return; }
			case 0xDB: { consume(1); size_t l = read_uint32(); consume(l); return; }
			case 0xC4: { consume(1); size_t l = read_uint8();  consume(l); return; }
			case 0xC5: { consume(1); size_t l = read_uint16(); consume(l); return; }
			case 0xC6: { consume(1); size_t l = read_uint32(); consume(l); return; }

			// Variable Arrays
			case 0xDC: { 
				size_t elements = consume_msgpack_header_and_get_len(type);
				for (size_t i = 0; i < elements; ++i) peek_and_skip_object();
				return;
			}
			case 0xDD: {
				size_t elements = consume_msgpack_header_and_get_len(type);
				for (size_t i = 0; i < elements; ++i) peek_and_skip_object();
				return;
			}

			// Variable Maps
			case 0xDE:
			case 0xDF: {
				size_t pairs = consume_msgpack_header_and_get_len(type);
				for (size_t i = 0; i < pairs * 2; ++i) peek_and_skip_object();
				return;
			}

			// FixExt Types
			case 0xD4: consume(3); return;  // fixext 1
			case 0xD5: consume(4); return;  // fixext 2
			case 0xD6: consume(6); return;  // fixext 4
			case 0xD7: consume(10); return; // fixext 8 (EventTime fits here)
			case 0xD8: consume(18); return; // fixext 16

			default:
				throw std::runtime_error("Malformed MessagePack stream token encountered");
		}
	}
    
	// Decodes MsgPack header lengths dynamically (FixStr, Str8/16/32, Bin8/16/32, Array, Map)
	size_t consume_msgpack_header_and_get_len(uint8_t type_byte){
		// 1. Consume the marker/type byte we just checked
		consume(1);

		// --- STRINGS (FixStr, Str8, Str16, Str32) ---
		if ((type_byte & 0xE0) == 0xA0) { 
			// FixStr (0xA0 - 0xBF): Length is embedded in the lower 5 bits
			return type_byte & 0x1F;
		}
		if (type_byte == 0xD9) return read_uint8();  // Str 8
		if (type_byte == 0xDA) return read_uint16(); // Str 16
		if (type_byte == 0xDB) return read_uint32(); // Str 32

		// --- ARRAYS (FixArray, Array16, Array32) ---
		if ((type_byte & 0xF0) == 0x90) {
			// FixArray (0x90 - 0x9F): Element count in lower 4 bits
			return type_byte & 0x0F;
		}
		if (type_byte == 0xDC) return read_uint16(); // Array 16
		if (type_byte == 0xDD) return read_uint32(); // Array 32

		// --- MAPS (FixMap, Map16, Map32) ---
		if ((type_byte & 0xF0) == 0x80) {
			// FixMap (0x80 - 0x8F): Key-Value pair count in lower 4 bits
			return type_byte & 0x0F;
		}
		if (type_byte == 0xDE) return read_uint16(); // Map 16
		if (type_byte == 0xDF) return read_uint32(); // Map 32

		// --- BINARY DATA (Bin8, Bin16, Bin32) ---
		if (type_byte == 0xC4) return read_uint8();  // Bin 8
		if (type_byte == 0xC5) return read_uint16(); // Bin 16
		if (type_byte == 0xC6) return read_uint32(); // Bin 32

		// If it's a fixed primitive (true, false, null, single float, or inline int)
		// the header is the value itself. We return 0 or an indicator.
		return 0;
	}
		
private:
	uint8_t read_uint8() {
		uint8_t val = 0;
		if (peek_byte(val, 0)){
			;
		}
		consume(1);
		return val;
	}

	uint16_t read_uint16() {
		uint16_t val;
		// Fast path: if contiguous inside the current temporary_buffer fragment
		std::string_view contiguous;
		auto ret = try_get_contiguous(2, contiguous);
		if (ret == 0) {
			val = *reinterpret_cast<const uint16_t*>(contiguous.data());
		} else if (ret == 1){
			// Slow path fallback: split across network buffers
			char bytes[2];
			read_split(bytes, 2);
			val = *reinterpret_cast<const uint16_t*>(bytes);
		}
		// MessagePack ints are big-endian; swap to host format (little-endian)
		return std::byteswap(val);
	}
	uint32_t read_uint32() {
		uint32_t val;
		std::string_view contiguous;
		auto ret = try_get_contiguous(4, contiguous);
		if (ret == 0) {
			val = *reinterpret_cast<const uint32_t*>(contiguous.data());
		} else if (ret == 1){
			char bytes[4];
			read_split(bytes, 4);
			val = *reinterpret_cast<const uint32_t*>(bytes);
		}
		return std::byteswap(val);
	}
	seastar::net::packet _packet_chain;
	size_t _frag_idx = 0;
	size_t _frag_offset = 0;
	size_t _global_offset = 0;
};

class ProtocolEngine {
public:
	ProtocolEngine(PacketCursor& cursor): _cursor(cursor){}
	int process_incoming_packet() {
        
		// 1. Root container verification (Fluent Forward root must be a MsgPack array)
		uint8_t root_byte = 0;
		if (_cursor.peek_byte(root_byte, 0)){
			return 1;
		}
		if ((root_byte & 0xF0) != 0x90 && root_byte != 0xDC && root_byte != 0xDD) {
			return -1; // Corrupt payload drop
		}
		size_t root_array_size = _cursor.consume_msgpack_header_and_get_len(root_byte);

		// 2. Extract Tag String (Always the first element)
		uint8_t tag_byte = 0;
		if (_cursor.peek_byte(tag_byte, 0)){
			return 1;
		}
		size_t tag_len = _cursor.consume_msgpack_header_and_get_len(tag_byte);
		std::string_view tag;
		char tag_backed[tag_len + 1];
		fmt::print("tag_len {}\n",tag_len);
		auto ret = _cursor.try_get_contiguous(tag_len, tag);
		if (ret == 1){
			fmt::print("{} {}\n",__FILE__,__LINE__);
			if (_cursor.read_split(tag_backed, tag_len) == -1){
				fmt::print("{} {}\n",__FILE__,__LINE__);
				return 1;
			}
			tag = std::string_view(tag_backed, tag_len);
		}else if (ret == -2){
			fmt::print("{} {}\n",__FILE__,__LINE__);
			return -1;
		}else if (ret == -1){
			fmt::print("{} {} waiting for more bytes\n",__FILE__,__LINE__);
			return 1;
		}
		fmt::print("tag len {}\n",tag_len);
		for(auto i = 0; i < tag_len; i++){
			fmt::print("{}\n",tag.data()[i]);
		}

		// 3. Inspect Second Element to Determine Format Mode
		uint8_t format_byte = 0;
		if (_cursor.peek_byte(format_byte, 0)){
			fmt::print("{} {}\n",__FILE__,__LINE__);
			return 1;
		}
		ForwardFormat mode;

		// Check for Message Mode: Second element is an Integer (Fixnum/Uint/Int) or Ext family
		if ((format_byte & 0x80) == 0 || (format_byte & 0xE0) == 0xE0 || format_byte == 0xCC || 
			format_byte == 0xCD || format_byte == 0xCE || format_byte == 0xCF || 
			(format_byte >= 0xD4 && format_byte <= 0xD8) || (format_byte >= 0xC7 && format_byte <= 0xC9)) {
			mode = ForwardFormat::Message;
		}
		// Check for Forward Mode: Second element is an Array containing [time, record] sub-entries
		else if ((format_byte & 0xF0) == 0x90 || format_byte == 0xDC || format_byte == 0xDD) {
			mode = ForwardFormat::Forward;
		}
		// Check for PackedForward Mode: Second element is a binary blob or string format container
		else if ((format_byte & 0xE0) == 0xA0 || (format_byte >= 0xDB && format_byte <= 0xDF) || 
			(format_byte >= 0xC4 && format_byte <= 0xC6)) {
			mode = ForwardFormat::PackedForward;
		} else {
			fmt::print("{} {}\n",__FILE__,__LINE__);
			return -1; // Unknown signature block drop
		}
fmt::print("{} {} {} {}\n",__FILE__,__LINE__,tag,(int)mode);
		// 4. Mode Processing & Telemetry Signal Disambiguation
		if (mode == ForwardFormat::Message) {
			// Layout: [tag, time, record, option?]
			parse_single_message(tag);
		} else if (mode == ForwardFormat::Forward) {
			// Layout: [tag, entries_array, option?]
			size_t entries_count = _cursor.consume_msgpack_header_and_get_len(format_byte);
            
			// Advance cursor past entries array to locate the third element (Option Map)
			// If option map is missing or lacks "fluent_signal", defaults strictly to Logs
			TelemetrySignal signal = extract_signal_from_options(root_array_size, 3);
            
			parse_forward_array(tag, entries_count, signal);
		} else if (mode == ForwardFormat::PackedForward) {
		// Layout: [tag, entries_bin_or_str, option]
			size_t binary_len = _cursor.consume_msgpack_header_and_get_len(format_byte);
			std::string_view raw_entries_blob;
			char raw_entries_blob_backed[binary_len + 1];
			auto ret = _cursor.try_get_contiguous(binary_len, raw_entries_blob);
			if (ret == 1){
				if (_cursor.read_split(raw_entries_blob_backed, tag_len) == -1){
					fmt::print("{} {}\n",__FILE__,__LINE__);
					return 1;
				}
				raw_entries_blob = std::string_view(raw_entries_blob_backed, binary_len);
			}else if (ret == -2){
				fmt::print("{} {}\n",__FILE__,__LINE__);
				return -1;
			}else if (ret == -1){
				fmt::print("{} {} waiting for more bytes\n",__FILE__,__LINE__);
				return 1;
			}

			// Access option map to check compression variables and optional signal declarations
			bool compressed = false;
			TelemetrySignal signal = TelemetrySignal::Logs; // Standard Fallback
            
			inspect_packed_options(root_array_size, compressed, signal);

			if (compressed) {
				// Background decompression lane
				// seastar::temporary_buffer<char> decompressed = Decompressor::inflate(raw_entries_blob);
				// parse_stream_by_signal(decompressed.get(), decompressed.size(), signal);
			} else {
				// Inline zero-copy lane
				parse_stream_by_signal(raw_entries_blob.data(), raw_entries_blob.size(), signal);
			}
		}
		return 0;
	}

private:
	void parse_single_message(std::string_view tag){
		fmt::print("{} {}\n",__FILE__,__LINE__);
	}
	void parse_forward_array(std::string_view tag, size_t entries_count, TelemetrySignal sig){
		for (size_t i = 0; i < entries_count; ++i) {
			// Each entry MUST be an array: [time, record]
			uint8_t entry_array_byte = 0;
			auto ret = _cursor.peek_byte(entry_array_byte, 0);
			if ((entry_array_byte & 0xF0) != 0x90 && entry_array_byte != 0xDC) {
				fmt::print("{} {}\n",__FILE__,__LINE__);
				return; // Corrupt sub-entry boundary, halt execution loop
			}
			size_t sub_array_len = _cursor.consume_msgpack_header_and_get_len(entry_array_byte);
			if (sub_array_len < 2){
				fmt::print("{} {}\n",__FILE__,__LINE__);
			       return; // Malformed layout
			}

			// --- 1. Parse/Skip Timestamp ---
			uint8_t time_type = 0;
			if (_cursor.peek_byte(time_type, 0)){
				fmt::print("{} {}\n",__FILE__,__LINE__);
				return;
			}
			int64_t timestamp = 0;

			if ((time_type & 0x80) == 0 || (time_type & 0xE0) == 0xE0 || time_type == 0xCC || time_type == 0xCD || time_type == 0xCE || time_type == 0xCF) {
				// Standard 32-bit Integer / Unix epoch timestamp
				size_t time_len = _cursor.consume_msgpack_header_and_get_len(time_type);
				// In a production ingestor, extract timestamp value here if sinking to ClickHouse columns
				_cursor.consume(time_len); 
			} else if (time_type == 0xD7) { 
				// EventTime Extension family (FixExt8 with type code 0)
				_cursor.consume(1); // Consume 0xD7 type marker
				uint8_t ext_type = 0;
				if (_cursor.peek_byte(ext_type, 0)){ // Should be 0x00 (EventTime)
					fmt::print("{} {}\n",__FILE__,__LINE__);
					return;
				}
				_cursor.consume(1); 
				// EventTime contains 4 bytes of seconds + 4 bytes of nanoseconds
				_cursor.consume(8); 
			}

			// --- 2. Extract and Parse the Record Map ---
			uint8_t record_map_byte = 0;
			if (_cursor.peek_byte(record_map_byte, 0)){
				fmt::print("{} {}\n",__FILE__,__LINE__);
				return;
			}
           
			// Capture the precise byte position where the Map starts
			size_t map_start_offset = _cursor.offset();
            
			// Advance the cursor past the map header to discover the key-value pairs count
			size_t map_pairs_count = _cursor.consume_msgpack_header_and_get_len(record_map_byte);

			// Skip over the map elements using our zero-copy length tracker 
			// to find out where this individual record ends in the packet stream
			size_t map_payload_len = calculate_msgpack_map_body_length(_cursor, map_pairs_count);
            
			// Rewind or calculate the slice boundary
			size_t map_total_bytes = (_cursor.offset() - map_start_offset) + map_payload_len;
            
			// Fetch the contiguous slice of memory representing just this record map
			std::string_view raw_map_slice;
			char raw_map_slice_backed[map_total_bytes];
			ret = _cursor.try_get_contiguous(map_total_bytes,raw_map_slice);
			if (ret == 1){
				if (_cursor.read_split(raw_map_slice_backed, map_total_bytes)){
					fmt::print("{} {}\n",__FILE__,__LINE__);
					return;
				}
				raw_map_slice = std::string_view(raw_map_slice_backed, map_total_bytes);
			}else if (ret == -2){
				fmt::print("{} {}\n",__FILE__,__LINE__);
				return;
			}

			if (!raw_map_slice.empty()) {
				// 3. Populate our stack-allocated lookahead array index trees
				StackLogFrame parsed_index = parse_msgpack_map_zero_copy(raw_map_slice);
                
				// 4. Dispatch the index references + packet lifecycle to core-local sinks
				// core_local_buffer.push_record(current_native_pkt, parsed_index, map_total_bytes);
			} else {
				// Edge case: Record map splits hard across TCP packets, handle via partial slice boundary fallback
				handle_split_map_record(_cursor, map_total_bytes);
			}
		}
	}
	void inspect_packed_options(size_t total_elements, bool& out_compressed, TelemetrySignal& out_signal) {
		// Fluent Forward specs dictate options exist only if root array length contains 3 elements
		if (total_elements < 3) {
			out_compressed = false;
			out_signal = TelemetrySignal::Logs;
			fmt::print("{} {}\n",__func__,__LINE__);
			return;
		}

		uint8_t map_byte = 0;
		if (_cursor.peek_byte(map_byte, 0)){
			fmt::print("{} {}\n",__func__,__LINE__);
		}
		// Verify token indicates a valid MsgPack map structure
		if ((map_byte & 0xF0) != 0x80 && map_byte != 0xDE && map_byte != 0xDF) {
			out_compressed = false;
			out_signal = TelemetrySignal::Logs;
			fmt::print("{} {}\n",__func__,__LINE__);
			return;
		}

		size_t map_pairs = _cursor.consume_msgpack_header_and_get_len(map_byte);

		for (size_t i = 0; i < map_pairs; ++i) {
			// 1. Extract Key
			uint8_t key_byte = 0;
			if (_cursor.peek_byte(key_byte, 0)){
				fmt::print("{} {}\n",__func__,__LINE__);
				return;
			}
			size_t key_len = _cursor.consume_msgpack_header_and_get_len(key_byte);
			std::string_view key;
			char key_backed[key_len + 1];
			auto ret = _cursor.try_get_contiguous(key_len, key);
			if (ret == 1){
				if (_cursor.read_split(key_backed, key_len)){
					fmt::print("{} {}\n",__func__,__LINE__);
					return;
				}
				key = std::string_view(key_backed, key_len);
			}else if (ret < 0){
				fmt::print("{} {}\n",__func__,__LINE__);
				return;
			}

			// 2. Extract Value depending on matching key literal flags
			if (key == "compressed") {
				uint8_t val_byte = 0;
				if (_cursor.peek_byte(val_byte, 0)){
					fmt::print("{} {}\n",__func__,__LINE__);
					return;
				}
				_cursor.consume(1); // Advance past token
                
				if (val_byte == 0xC3) { // MsgPack true token literal
					out_compressed = true;
				} else if (val_byte == 0xA4 || val_byte == 0xD9) { 
					// String check: string matching "gzip" or "zstd"
					size_t str_len = (val_byte == 0xD9) ? _cursor.consume_msgpack_header_and_get_len(val_byte) : (val_byte & 0x1F);
		        	        std::string_view compression_algo;
					char compression_algo_backed[str_len + 1];
					ret = _cursor.try_get_contiguous(str_len, compression_algo);
					if (ret == 1){
						if (_cursor.read_split(compression_algo_backed, str_len)){
							fmt::print("{} {}\n",__func__,__LINE__);
							return;
						}
						compression_algo = std::string_view(compression_algo_backed, str_len);
					}else if (ret < 0){
						fmt::print("{} {}\n",__func__,__LINE__);
						return;
					}
                			if (compression_algo == "gzip") {
						out_compressed = true;
					}
				} else {
					out_compressed = false;
				}
			} else if (key == "fluent_signal") {
				uint8_t val_byte = 0;
				if (_cursor.peek_byte(val_byte, 0)){
					fmt::print("{} {}\n",__func__,__LINE__);
					return;
				}
				// Extract value integer safely inline
				if ((val_byte & 0x80) == 0) { // Positive fixed integer
					out_signal = static_cast<TelemetrySignal>(val_byte & 0x7F);
					_cursor.consume(1);
				} else if (val_byte == 0xCC) { // Uint8 descriptor
					_cursor.consume(1);
					uint8_t tmp;
					if (_cursor.peek_byte(tmp,0)){
						fmt::print("{} {}\n",__func__,__LINE__);
						return;
					}
					out_signal = static_cast<TelemetrySignal>(tmp);
					_cursor.consume(1);
				} else {
					out_signal = TelemetrySignal::Logs; // Fallback anomaly protector
					_cursor.peek_and_skip_object(); // Step over safely
				}
			} else {
				// Unknown meta fields (e.g., "size", "chunk" authorizations) -> step past safely
				_cursor.peek_and_skip_object();
			}
		}
		fmt::print("{} {}\n",__func__,__LINE__);
	}

				// Recursively jumps over MsgPack elements to calculate exact boundaries
	size_t calculate_msgpack_map_body_length(PacketCursor& cursor, size_t pairs_count) {
		// 1. Mark our exact initial cursor position right after the Map header
		size_t initial_offset = _cursor.offset();

		// 2. Loop through every key-value pair and step the cursor past them
		for (size_t i = 0; i < pairs_count; ++i) {
			// Skip Key
			_cursor.peek_and_skip_object();
            
			// Skip Value (This handles nested maps, strings, ints, etc. automatically)
			_cursor.peek_and_skip_object();
		}

		// 3. The difference in offsets is exactly the body length in bytes
		size_t final_offset = cursor.offset();
		fmt::print("{} {} {}\n",__func__,__LINE__,final_offset - initial_offset);
		return final_offset - initial_offset;
	}
	void handle_split_map_record(PacketCursor& cursor, size_t total_bytes){
		fmt::print("{} {}\n",__func__,__LINE__);
	}
	void parse_stream_by_signal(const char* data, size_t len, TelemetrySignal sig){
		fmt::print("{} {}\n",__func__,__LINE__);
	}
    
	TelemetrySignal extract_signal_from_options(size_t total_elements, size_t target_idx){
		return TelemetrySignal::Logs; 
	}
	PacketCursor& _cursor;
};

}
