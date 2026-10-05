#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_af_helper.hh"
#include "rapid_ingestor_stats.hh"

namespace RapidIngestor{

	class Protocol;

	class Listener : public seastar::enable_lw_shared_from_this<Listener> {
		public:
			Listener(){}
			~Listener(){fmt::print("{} {}\n",__func__,__LINE__);}
			seastar::future<> listen(const seastar::sstring&ip, uint16_t port);
			seastar::future<> onProtocolDone(seastar::lw_shared_ptr<Protocol> protocol);
			RapidIngestorStats getStats();
		private:
			std::shared_ptr<AfHelper> _afHelper;
			std::unordered_map<seastar::socket_address, seastar::lw_shared_ptr<Protocol>> _protocols;
			RapidIngestorStats _stats;
	};
}

#include "rapid_ingestor_listener.inl"
