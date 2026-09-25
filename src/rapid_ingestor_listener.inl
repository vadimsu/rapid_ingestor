
#pragma once
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_sharded_handler.hh"

namespace RapidIngestor{

seastar::future<> Listener::listen(const seastar::sstring&ip, uint16_t port,seastar::sharded<ShardedHandler>& handlers){
	_afHelper = std::make_shared<RapidIngestor::TcpAfHelper>(ip, port);
	return seastar::do_until([this]{
		return false;
	},
	[this, &handlers]{
		return _afHelper->accept().then([this, &handlers] (auto ar) mutable {
			seastar::connected_socket fd = std::move(ar.connection);
			seastar::socket_address addr = std::move(ar.remote_address);
			// Round-robin or hash-based target shard assignment
			static unsigned next_shard = 1;
			unsigned target_shard = next_shard++;
			if (next_shard == seastar::smp::count){
				next_shard = 1;
			}

			// Move the socket ownership to the chosen shard
//			fmt::print("scheduling connection to {} out of {}\n",target_shard,seastar::smp::count);
			(void) seastar::smp::submit_to(target_shard, [this, s = std::move(fd), a = std::move(addr), &handlers]() mutable {
				return handlers.local().handle_connection(std::move(s), std::move(a));
			});
		});
	});
}

}
