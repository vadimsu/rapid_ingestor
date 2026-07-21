#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_message_parser.hh"

namespace RapidIngestor{

	class Protocol : public seastar::enable_lw_shared_from_this<Protocol> {
		public:
			Protocol(seastar::socket_address addr): _addr(addr){}
			~Protocol(){fmt::print("{} {}\n",__func__,__LINE__);}
			void onAccepted(seastar::lw_shared_ptr<Connection> connection){
				_connection = connection;
				seastar::do_until([this, this_proto = this->shared_from_this()] {
							return !_connection->isAlive();
						},
						[this, this_proto = this->shared_from_this()]{
							return _connection->receive().then([this] (seastar::temporary_buffer<char> tb){
									fmt::print("[DEBUG] {} received buffer size={}\n",_addr, tb.size());
									_packetCursor.append(std::move(tb));
									process_accumulated_bytes();
								});
						});
			}
		private:
			void process_accumulated_bytes() {
				ProtocolEngine protocolEngine(_packetCursor);
				auto ret = protocolEngine.process_incoming_packet();
				fmt::print("[DEBUG] {} parser returned {} offset={} remaining={}\n",_addr,
					ret,
					_packetCursor.offset(),
					_packetCursor.remaining());
				if (ret == -1) {
					fmt::print("fatal parsing/format error\n");
				} else if (ret == 0) {
					// Successful parse - check if all bytes consumed
					// If so, reset cursor to free packet memory
					// Otherwise, remaining data is partial next message, keep it
					if (_packetCursor.remaining() == 0) {
						fmt::print("[DEBUG] {} all bytes consumed, resetting cursor to free packet memory\n",_addr);
						_packetCursor.reset();
					} else {
						fmt::print("[DEBUG] {} {} bytes remaining for next message\n",_addr, _packetCursor.remaining());
					}
				} else {
					fmt::print("{} parsing result {}\n",_addr, ret);
				}
			}
			seastar::lw_shared_ptr<Connection> _connection;
			seastar::socket_address _addr;
			PacketCursor _packetCursor;
	};
}
