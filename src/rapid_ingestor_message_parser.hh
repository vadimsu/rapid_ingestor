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
	int peek_byte(uint8_t& dst, size_t ahead = 0) const{
		if  (_packet_chain.len() == 0){
			return 1;
		}
		const auto& frag = _packet_chain.frag(0);
//		const auto frag = frags[0];
		if (frag.size < 1){
			return 1;
		}
		dst = frag.base[0];
		return 0;
	}
	// Fast-path lookup: returns a direct string_view if fully contained in current fragment
	int try_get_contiguous(size_t len, std::string_view& sv) {
		if (remaining() < len) return -1;//wait for more bytes
        	
		if (_frag_idx >= _packet_chain.nr_frags()) return -2;//fatal
		const auto& cur_frag = _packet_chain.frag(_frag_idx);

        	size_t frag_rem = cur_frag.size - _frag_offset;

		if (frag_rem >= len) {
			fmt::print("{} {} {} {}\n",__FILE__,__LINE__,_frag_offset, len);
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
	void consume(size_t len){
		_global_offset += len;
		_frag_offset += len;
        
		while (_frag_idx < _packet_chain.nr_frags() && _frag_offset >= _packet_chain.frag(_frag_idx).size) {
			_frag_offset -= _packet_chain.frag(_frag_idx).size;
			_frag_idx++;
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
	size_t parsed_offset = 0;
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
			fmt::print("{} {}\n",__FILE__,__LINE__);
			return 1;
		}
		if ((root_byte & 0xF0) != 0x90 && root_byte != 0xDC && root_byte != 0xDD) {
			fmt::print("{} {}\n",__FILE__,__LINE__);
			return -1; // Corrupt payload drop
		}
		size_t root_array_size = _cursor.consume_msgpack_header_and_get_len(root_byte);

		// 2. Extract Tag String (Always the first element)
		uint8_t tag_byte = 0;
		if (_cursor.peek_byte(tag_byte, 0)){
			fmt::print("{} {}\n",__FILE__,__LINE__);
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
			if (ret == -1){
				if (_cursor.read_split(raw_entries_blob_backed, tag_len) == -1){
					fmt::print("{} {}\n",__FILE__,__LINE__);
					return 1;
				}
				raw_entries_blob = std::string_view(raw_entries_blob_backed, binary_len);
			}else if (ret == -2){
				return -1;
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
	}
	void parse_forward_array(std::string_view tag, size_t count, TelemetrySignal sig){
	}
	void parse_stream_by_signal(const char* data, size_t len, TelemetrySignal sig){
	}
    
	TelemetrySignal extract_signal_from_options(size_t total_elements, size_t target_idx){
		return TelemetrySignal::Logs; 
	}
	void inspect_packed_options(size_t total_elements, bool& out_compressed, TelemetrySignal& out_signal){
	}
	PacketCursor& _cursor;
};

}
