#pragma once

#include "rapid_ingestor_listener.hh"

namespace RapidIngestor{

seastar::future<RapidIngestorStats> Protocol::onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener){
	_connection = connection;
	_listener = listener;

	return seastar::do_until([this, this_proto = this->shared_from_this()] {
				return !_connection->isAlive() || _fatal_error;
			},
			[this, this_proto = this->shared_from_this()]{
				return _connection->receive().then([this] (seastar::temporary_buffer<char> tb){
//						fmt::print("[DEBUG] {} received buffer size={}\n",_addr, tb.size());
					_stats.bytesProcessed += tb.size();
					_protocolEngine.append(std::move(tb));
					process_accumulated_bytes();
					// Keep the live count visible via getStats() while the
					// connection is open, not just once it closes.
					_stats.messagesParsed = _protocolEngine.getElementCount();
				});
			}).then([this]{
				return _listener->onProtocolDone(this->shared_from_this()).then([this_proto = this->shared_from_this()] {
					this_proto->_stats.messagesParsed = this_proto->_protocolEngine.getElementCount();
					return seastar::make_ready_future<RapidIngestorStats>(this_proto->getStats());
				});
			});
}

void Protocol::process_accumulated_bytes() {
	// Drain every complete message that is already buffered in one go —
	// otherwise, under sustained high throughput, complete-but-unparsed
	// messages pile up in the packet chain (which can only be freed by
	// destroying/compacting it) and memory grows without bound.
	for (;;) {
		auto ret = _protocolEngine.process_incoming_packet();
		if (ret == -1) {
			fmt::print("fatal parsing/format error on {}, closing connection\n", _addr);
			_fatal_error = true;
			break;
		}
		if (ret == 1) {
			// Not enough bytes yet for the next message — compact away
			// already-consumed fragments so the chain doesn't grow forever
			// while we wait for the rest to arrive.
			_protocolEngine.compact();
			break;
		}
		// ret == 0: one message parsed; loop in case more are buffered.
		if (_protocolEngine._cursor.remaining() == 0) {
			_protocolEngine._cursor.reset();
			break;
		}
	}
}

RapidIngestorStats& Protocol::getStats(){
	//fmt::print("{} {} {} {} {} {}\n",__FILE__,__func__,__LINE__,_stats.messagesParsed,_stats.bytesProcessed,_addr);
	return _stats;
}

seastar::future<> Protocol::stop(){
	return _connection->stop().then([this]{
			return seastar::make_ready_future<>();
	});
}

}
