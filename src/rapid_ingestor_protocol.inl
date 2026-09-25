#pragma once

#include "rapid_ingestor_listener.hh"

namespace RapidIngestor{

thread_local uint64_t _messagesParsed = 0;

seastar::future<uint64_t> Protocol::onAccepted(seastar::lw_shared_ptr<Connection> connection, Listener* listener){
	_connection = connection;
	_listener = listener;
	return seastar::do_until([this, this_proto = this->shared_from_this()] {
				return !_connection->isAlive();
			},
			[this, this_proto = this->shared_from_this()]{
				return _connection->receive().then([this] (seastar::temporary_buffer<char> tb){
//						fmt::print("[DEBUG] {} received buffer size={}\n",_addr, tb.size());
					_protocolEngine.append(std::move(tb));
					process_accumulated_bytes();
				});
			}).then([this]{
				return _listener->onProtocolDone(this->shared_from_this());
//				return stop();
//				return seastar::make_ready_future<>();
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
		_messageCount++;
	}
}

seastar::future<uint64_t> Protocol::stop(){
	return _connection->stop().then([this]{
			return seastar::make_ready_future<uint64_t>(_messageCount);
	});
}

}
