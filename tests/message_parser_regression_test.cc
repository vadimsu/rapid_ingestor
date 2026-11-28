#include <iostream>
#include <string>

#include "src/rapid_ingestor_message_parser.hh"

int main() {
    using namespace RapidIngestor;

    {
        std::string full_bytes{static_cast<char>(0x81), static_cast<char>(0xA1), 'a', static_cast<char>(0x01)};
        seastar::temporary_buffer<char> tb(full_bytes.data(), full_bytes.size());
        PacketCursor cursor;
        cursor.append(std::move(tb));
        std::cout << "about to test full object" << std::endl;
        cursor.debug_cursor();
        auto st = cursor.peek_and_skip_object();
        if (st != ParserStatus::Ok) {
            std::cerr << "expected full object to parse successfully\n";
            return 1;
        }
    }

    {
        std::string partial_bytes{static_cast<char>(0x81), static_cast<char>(0xA1), 'a'};
        seastar::temporary_buffer<char> tb(partial_bytes.data(), partial_bytes.size());
        PacketCursor cursor;
        cursor.append(std::move(tb));
        std::cout << "about to test partial object" << std::endl;
        cursor.debug_cursor();
        auto st = cursor.peek_and_skip_object();
        if (st != ParserStatus::NeedMoreData) {
            std::cerr << "expected partial object to require more data\n";
            return 1;
        }
    }

    {
        std::string partial_bytes{static_cast<char>(0x81), static_cast<char>(0xA1), 'a'};
        seastar::temporary_buffer<char> tb(partial_bytes.data(), partial_bytes.size());
        PacketCursor cursor;
        cursor.append(std::move(tb));
        std::cout << "about to test protocol engine" << std::endl;
        cursor.debug_cursor();
        ProtocolEngine engine(cursor);
        auto ret = engine.process_incoming_packet();
        if (ret != 1) {
            std::cerr << "expected process_incoming_packet to return need-more-data for partial packets\n";
            return 1;
        }
    }

    std::cout << "parser regression checks passed\n";
    return 0;
}
