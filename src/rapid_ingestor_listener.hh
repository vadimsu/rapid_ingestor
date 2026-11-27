#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_af_helper.hh"
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_stats.hh"
#include "rapid_ingestor_clickhouse_sink.hh"

namespace RapidIngestor{

	class IProtocol;
	class ClickHouseSink;
	typedef enum {
		FORWARD_PROTOCOL
	}ProtocolType;

	class Listener : public seastar::enable_lw_shared_from_this<Listener> {
		public:
			Listener(): _protocol_type(FORWARD_PROTOCOL){
			}
			~Listener(){fmt::print("{} {}\n",__func__,__LINE__);}
			// `sink` is optional (database_type empty = no sink configured); when
			// present, a per-shard ClickHouseSink is started before accepting any
			// connections and wired into every accepted Protocol.
			seastar::future<> listen(seastar::sstring ip, uint16_t port, Sink sink, seastar::sstring protocol_type);
			seastar::future<> onProtocolDone(seastar::shared_ptr<IProtocol> protocol);
			RapidIngestorStats getStats();
		private:
			seastar::shared_ptr<AfHelper> _afHelper;
			std::unordered_map<seastar::socket_address, seastar::shared_ptr<IProtocol>> _protocols;
			RapidIngestorStats _stats;
			seastar::lw_shared_ptr<ClickHouseSink> _sink;
			ProtocolType _protocol_type;
	};
}
