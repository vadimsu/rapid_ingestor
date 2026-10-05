
#pragma once
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_protocol.hh"

namespace RapidIngestor{

seastar::future<> Listener::listen(const seastar::sstring&ip, uint16_t port){
	if (port == 0){//unix
		_afHelper = std::make_shared<RapidIngestor::UnixAfHelper>(ip, port);
	}else{
		_afHelper = std::make_shared<RapidIngestor::TcpAfHelper>(ip, port);
	}
	return seastar::do_until([this]{
		return false;
	},
	[this]{
		return _afHelper->accept().then([this] (auto ar) mutable {
			seastar::connected_socket fd = std::move(ar.connection);
			seastar::socket_address addr = std::move(ar.remote_address);
			auto protocol = seastar::make_lw_shared<Protocol>(addr);
			auto conn = seastar::make_lw_shared<Connection>(std::move(fd), addr);
//			fmt::print("accepted {}\n",addr);
			_protocols.emplace(protocol->getAddress(), protocol);
			protocol->onAccepted(conn, this).then([this] (RapidIngestorStats stats){
				_stats += stats;
				return seastar::make_ready_future<>();
			});
		});
	});
}

seastar::future<> Listener::onProtocolDone(seastar::lw_shared_ptr<Protocol> protocol){
	return protocol->stop().then([this, protocol] {
		_protocols.erase(protocol->getAddress());
		return seastar::make_ready_future<>();
	});
}

RapidIngestorStats Listener::getStats(){
	RapidIngestorStats current_stats;
	for (auto& p : _protocols){
		current_stats += p.second->getStats();
	}
	return _stats + current_stats;
}

}
