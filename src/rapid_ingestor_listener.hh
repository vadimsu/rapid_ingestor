#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_protocol.hh"
#include "rapid_ingestor_af_helper.hh"

namespace RapidIngestor{

class Connection;

	class Listener : public seastar::enable_lw_shared_from_this<Listener> {
		public:
			Listener(std::shared_ptr<AfHelper> afHelper): _afHelper(afHelper){}
			~Listener(){fmt::print("{} {}\n",__func__,__LINE__);}
			seastar::future<> listen(){
				return _afHelper->listen().then([this]{
						return seastar::do_until([this]{
									return false;
								},
								[this]{
									return _afHelper->accept().then([this] (auto ar) mutable {
											seastar::connected_socket fd = std::move(ar.connection);
											seastar::socket_address addr = std::move(ar.remote_address);
											auto protocol = seastar::make_lw_shared<Protocol>(addr);
							                                auto conn = seastar::make_lw_shared<Connection>(std::move(fd), addr);
											fmt::print("accepted {}\n",addr);
											_protocols.push_back(protocol);
											protocol->onAccepted(conn);
										});
								});
					});
			}
		private:
			std::shared_ptr<AfHelper> _afHelper;
			std::vector<seastar::lw_shared_ptr<Protocol>> _protocols;
	};
}
