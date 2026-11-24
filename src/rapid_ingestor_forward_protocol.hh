#pragma once

#include "rapid_ingestor_listener.hh"
#include "rapid_ingestor_protocol.hh"

namespace RapidIngestor{

class ForwardProtocol : public IProtocol {
public:
	ForwardProtocol(seastar::socket_address addr);

	~ForwardProtocol();

	seastar::future<RapidIngestorStats> onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener);

	void process_accumulated_bytes();

	const seastar::socket_address& getAddress();

	RapidIngestorStats& getStats() override;

	seastar::future<> stop() override;

	void setSink(ProtocolEngine::RecordSink sink);

	void setBackpressure(IsOverloaded isOverloaded, WaitForCapacity waitForCapacity);

private:
	seastar::socket_address _addr;
	seastar::lw_shared_ptr<Connection> _connection;
	Listener* _listener;
	bool _fatal_error;
	RapidIngestorStats _stats;
	ProtocolEngine _protocolEngine;
	IsOverloaded _isOverloaded;
	WaitForCapacity _waitForCapacity;
};


}
