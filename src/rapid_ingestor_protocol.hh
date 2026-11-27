#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_message_parser.hh"
#include "rapid_ingestor_stats.hh"

namespace RapidIngestor{

	class ShardedHandler;

	using IsOverloaded = std::function<bool()>;
	using WaitForCapacity = std::function<seastar::future<>()>;

	class IProtocol : public seastar::enable_shared_from_this<IProtocol>{
		public:
			virtual RapidIngestorStats& getStats() = 0;
			virtual seastar::future<> stop() = 0;
			virtual void setSink(ProtocolEngine::RecordSink sink) = 0;
			virtual void setBackpressure(IsOverloaded isOverloaded, WaitForCapacity waitForCapacity) = 0;
			virtual const seastar::socket_address& getAddress() = 0;
			virtual seastar::future<RapidIngestorStats> onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener) = 0;
	};
}
