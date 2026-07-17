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
									fmt::print("{} {} {}\n",__FILE__,__LINE__,tb.size());
									_packetCursor.append(std::move(tb));
									process_accumulated_bytes();
								});
						});
			}
		private:
			void process_accumulated_bytes() {
				ProtocolEngine protocolEngine(_packetCursor);
				auto ret = protocolEngine.process_incoming_packet();
				if  (ret == -1){
					fmt::print("fatal parsing/format error\n");
				}else{
					fmt::print("parsing result {}\n", ret);
				}
			}
			seastar::lw_shared_ptr<Connection> _connection;
			seastar::socket_address _addr;
			PacketCursor _packetCursor;
	};
}
