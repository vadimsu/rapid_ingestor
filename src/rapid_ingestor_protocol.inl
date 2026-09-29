#pragma once

#include "rapid_ingestor_listener.hh"

namespace RapidIngestor{

seastar::future<RapidIngestorStats> Protocol::onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener){
	_connection = connection;
	_listener = listener;

	return seastar::do_until([this, this_proto = this->shared_from_this()] {
				return !_connection->isAlive();
			},
			[this, this_proto = this->shared_from_this()]{
				return _connection->receive().then([this] (seastar::temporary_buffer<char> tb){
//						fmt::print("[DEBUG] {} received buffer size={}\n",_addr, tb.size());
					_stats.bytesProcessed += tb.size();
					_protocolEngine.append(std::move(tb));
					process_accumulated_bytes();
				});
			}).then([this]{
				return _listener->onProtocolDone(this->shared_from_this()).then([this_proto = this->shared_from_this()] {
					this_proto->_stats.messagesParsed = this_proto->_protocolEngine.getElementCount();
					return seastar::make_ready_future<RapidIngestorStats>(this_proto->getStats());
				});
			});
}

void Protocol::process_accumulated_bytes() {
	auto ret = _protocolEngine.process_incoming_packet();
//	fmt::print("[DEBUG] {} parser returned {} offset={} remaining={}\n",_addr,
//		ret,
//		_protocolEngine._cursor.offset(),
//		_protocolEngine._cursor.remaining());
	if (ret == -1) {
		fmt::print("fatal parsing/format error\n");
	} else if (ret == 0) {
		if (_protocolEngine._cursor.remaining() == 0) {
//			fmt::print("[DEBUG] {} all bytes consumed, resetting cursor to free packet memory\n",_addr);
			_protocolEngine._cursor.reset();
		} else {
//			fmt::print("[DEBUG] {} {} bytes remaining for next message\n",_addr, _protocolEngine._cursor.remaining());
		}
	} else {
//		fmt::print("{} parsing result {}\n",_addr, ret);
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
