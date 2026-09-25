#pragma once

#include<unordered_map>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>

namespace RapidIngestor {

class Connection;
class Protocol;

	class ShardedHandler {
		public:
			seastar::future<> handle_connection(seastar::connected_socket socket, seastar::socket_address sa);
			seastar::future<> onProtocolDone(seastar::lw_shared_ptr<Protocol> protocol);
		private:
			std::unordered_map<seastar::socket_address, seastar::lw_shared_ptr<Protocol>> _protocols;
	};
}

#include "rapid_ingestor_sharded_handler.inl"
