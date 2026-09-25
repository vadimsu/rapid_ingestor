#pragma once

#include "rapid_ingestor_listener.hh"

namespace RapidIngestor{

thread_local uint64_t _messagesParsed = 0;

void Protocol::onAccepted(seastar::lw_shared_ptr<Connection> connection, seastar::lw_shared_ptr<Listener> listener){
	_connection = connection;
	_listener = listener;
	seastar::do_until([this, this_proto = this->shared_from_this()] {
				return !_connection->isAlive();
			},
			[this, this_proto = this->shared_from_this()]{
				return _connection->receive().then([this] (seastar::temporary_buffer<char> tb){
//						fmt::print("[DEBUG] {} received buffer size={}\n",_addr, tb.size());
					_protocolEngine.append(std::move(tb));
					process_accumulated_bytes();
				});
			}).then([this]{
				_listener->onProtocolDone(this->shared_from_this());
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
		_messagesParsed++;
		if (_messagesParsed % 1000 == 0){
			fmt::print("{} messages\n",_messagesParsed);
		}
	}
}

seastar::future<> Protocol::stop(){
	return _connection->stop();
}

}
