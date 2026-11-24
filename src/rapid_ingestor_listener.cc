
#include "rapid_ingestor_connection.hh"
#include "rapid_ingestor_unix_af_helper.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_otel.hh"
#include "rapid_ingestor_forward_protocol.hh"
#include "rapid_ingestor_clickhouse_sink.hh"

namespace RapidIngestor{

seastar::future<> Listener::listen(seastar::sstring ip, uint16_t port, Sink sink, seastar::sstring protocol_type){
	fmt::print("{} {} {} {}\n",__func__,__LINE__,sink.mode,sink.database_type);
	if (!sink.database_type.empty()){
		_sink = seastar::make_lw_shared<ClickHouseSink>(sink);
		fmt::print("starting sink\n");
		co_await _sink->start();
	}
	if (port == 0){//unix
		fmt::print("removing unix domain socket file {}\n",ip);
		seastar::remove_file(fmt::format("{}", ip));
		_afHelper = seastar::make_shared<RapidIngestor::UnixAfHelper>(ip, port);
	}else{
		_afHelper = seastar::make_shared<RapidIngestor::TcpAfHelper>(ip, port);
	}
	co_await seastar::do_until([this]{
		return false;
	},
	[this, protocol_type]{
		return _afHelper->accept().then([this, protocol_type] (auto ar) mutable {
			seastar::connected_socket fd = std::move(ar.connection);
			seastar::socket_address addr = std::move(ar.remote_address);
			seastar::shared_ptr<IProtocol> protocol;
			if (protocol_type == "forward"){
				protocol = seastar::make_shared<ForwardProtocol>(addr);
			}else if  (protocol_type == "otel"){
				protocol = seastar::make_shared<OtelProtocol>(addr);
			}
			fmt::print("Protocol type {}\n",protocol_type);
			auto conn = seastar::make_lw_shared<Connection>(std::move(fd), addr);
//			fmt::print("accepted {}\n",addr);
			if (_sink) {
				auto sink = _sink;
				protocol->setSink([sink] (std::string_view tag, const StackLogFrame& frame) {
					sink->add(tag, frame);
				});
				protocol->setBackpressure(
					[sink] { return sink->isOverloaded(); },
					[sink] { return sink->waitForCapacity(); });
			}
			_protocols.emplace(protocol->getAddress(), protocol);
			protocol->onAccepted(conn, this).then([this] (RapidIngestorStats stats){
				_stats += stats;
				return seastar::make_ready_future<>();
			});
		});
	});
}


seastar::future<> Listener::onProtocolDone(seastar::shared_ptr<IProtocol> protocol){
	return protocol->stop().then([this, protocol] {
		_protocols.erase(protocol->getAddress());
		return seastar::make_ready_future<>();
	});
}

RapidIngestorStats Listener::getStats(){
	RapidIngestorStats current_stats;
	for (auto& p : _protocols){
		current_stats += p.second->getStats();
	}
	current_stats.rowsInserted = _sink ? _sink->getRowsInserted() : 0;
	return _stats + current_stats;
}

}
