#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
//#include "rapid_ingestor_protocol.hh"
#include "rapid_ingestor_af_helper.hh"

namespace RapidIngestor{

class Connection;
class Protocol;

	class Listener : public seastar::enable_lw_shared_from_this<Listener> {
		public:
			Listener(std::shared_ptr<AfHelper> afHelper): _afHelper(afHelper){}
			~Listener(){fmt::print("{} {}\n",__func__,__LINE__);}
			seastar::future<> listen();
			seastar::future<> onProtocolDone(seastar::lw_shared_ptr<Protocol> protocol);
		private:
			std::shared_ptr<AfHelper> _afHelper;
			std::unordered_map<seastar::socket_address, seastar::lw_shared_ptr<Protocol>> _protocols;
	};
}

#include "rapid_ingestor_listener.inl"
