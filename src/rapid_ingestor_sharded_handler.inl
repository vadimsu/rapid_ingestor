
#pragma once

#include "rapid_ingestor_protocol.hh"

namespace RapidIngestor {

seastar::future<> ShardedHandler::handle_connection(seastar::connected_socket socket, seastar::socket_address sa){
	seastar::connected_socket fd = std::move(socket);
	seastar::socket_address addr = std::move(sa);
	auto protocol = seastar::make_lw_shared<Protocol>(addr);
	auto conn = seastar::make_lw_shared<Connection>(std::move(fd), addr);
//	fmt::print("accepted {}\n",addr);
	_protocols.emplace(protocol->getAddress(), protocol);
	(void)protocol->onAccepted(conn, this);
	return seastar::make_ready_future<>();
}

#if 1
seastar::future<> ShardedHandler::onProtocolDone(seastar::lw_shared_ptr<Protocol> protocol){
	return protocol->stop().then([this, protocol]{
		_protocols.erase(protocol->getAddress());
		return seastar::make_ready_future<>();
	});
}
#endif

}
