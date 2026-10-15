#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_message_parser.hh"
#include "rapid_ingestor_stats.hh"

export thread_local uint64_t _messagesParsed;

namespace RapidIngestor{

	class ShardedHandler;

	class Protocol : public seastar::enable_lw_shared_from_this<Protocol> {
		public:
			Protocol(seastar::socket_address addr): _addr(addr){}
			~Protocol(){/*fmt::print("{} {}\n",__func__,__LINE__);*/}
			seastar::future<RapidIngestorStats> onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener*);
			const seastar::socket_address& getAddress(){ return _addr; }
			seastar::future<> stop();
			RapidIngestorStats& getStats();
			void setSink(ProtocolEngine::RecordSink sink) { _protocolEngine.setSink(std::move(sink)); }
		private:
			void process_accumulated_bytes();
			seastar::socket_address _addr;
			seastar::lw_shared_ptr<Connection> _connection;
			ProtocolEngine _protocolEngine;
			Listener* _listener;
			RapidIngestorStats _stats;
			bool _fatal_error = false;
	};
}

#include "rapid_ingestor_protocol.inl"
