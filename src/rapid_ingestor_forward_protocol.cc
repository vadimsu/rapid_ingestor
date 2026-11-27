
#include "rapid_ingestor_forward_protocol.hh"

namespace RapidIngestor{

ForwardProtocol::ForwardProtocol(seastar::socket_address addr): _addr(addr), _listener(nullptr), _fatal_error(false){
	memset(&_stats, 0, sizeof(_stats));
}

ForwardProtocol::~ForwardProtocol(){
}

seastar::future<RapidIngestorStats> ForwardProtocol::onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener){
	_connection = connection;
	_listener = listener;

	return seastar::do_until([this] {
			return !_connection->isAlive() || _fatal_error;
		},
		[this]{
			auto do_receive = [this]{
				return _connection->receive().then([this] (seastar::temporary_buffer<char> tb){
//						fmt::print("[DEBUG] {} received buffer size={}\n",_addr, tb.size());
					_stats.bytesProcessed += tb.size();
					_protocolEngine.append(std::move(tb));
					process_accumulated_bytes();
					// Keep the live count visible via getStats() while the
					// connection is open, not just once it closes.
					_stats.messagesParsed = _protocolEngine.getElementCount();
					_stats.unparsedBufferSize = _protocolEngine.getUnparsedBufferSize();
				});
			};
			if (_isOverloaded && _isOverloaded()) {
				return _waitForCapacity().then(do_receive);
			}
			return do_receive();
		}).then([this]{
			return _listener->onProtocolDone(this->shared_from_this()).then([this] {
				_stats.messagesParsed = _protocolEngine.getElementCount();
				_stats.unparsedBufferSize = _protocolEngine.getUnparsedBufferSize();
				return seastar::make_ready_future<RapidIngestorStats>(getStats());
			});
		});
}

void ForwardProtocol::process_accumulated_bytes() {
	// Drain every complete message that is already buffered in one go —
	// otherwise, under sustained high throughput, complete-but-unparsed
	// messages pile up in the packet chain (which can only be freed by
	// destroying/compacting it) and memory grows without bound.
	for (;;) {
		auto ret = _protocolEngine.process_incoming_packet();
		if (ret == -1) {
			fmt::print("fatal parsing/format error on {}, closing connection\n", _addr);
			_fatal_error = true;
			_stats.fatalErrors++;
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

const seastar::socket_address& ForwardProtocol::getAddress(){
	return _addr;
}

RapidIngestorStats& ForwardProtocol::getStats() {
	//fmt::print("{} {} {} {} {} {}\n",__FILE__,__func__,__LINE__,_stats.messagesParsed,_stats.bytesProcessed,_addr);
	return _stats;
}

seastar::future<> ForwardProtocol::stop() {
	return _connection->stop().then([this]{
		return seastar::make_ready_future<>();
	});
}

void ForwardProtocol::setSink(ProtocolEngine::RecordSink sink){
	_protocolEngine.setSink(std::move(sink));
}

void ForwardProtocol::setBackpressure(IsOverloaded isOverloaded, WaitForCapacity waitForCapacity){
	_isOverloaded = isOverloaded;
       _waitForCapacity = waitForCapacity;
}

}
