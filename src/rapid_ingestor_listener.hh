#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_af_helper.hh"

namespace RapidIngestor{

	class ShardedHandler;

	class Listener : public seastar::enable_lw_shared_from_this<Listener> {
		public:
			Listener(){}
			~Listener(){fmt::print("{} {}\n",__func__,__LINE__);}
			seastar::future<> listen(const seastar::sstring&ip, uint16_t port,seastar::sharded<ShardedHandler>& handlers);
		private:
			std::shared_ptr<AfHelper> _afHelper;
//			seastar::sharded<ShardedHandler>& _handlers;
	};
}

#include "rapid_ingestor_listener.inl"
