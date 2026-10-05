#pragma once

#include <vector>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include "rapid_ingestor_af_helper.hh"

namespace RapidIngestor{

	class UnixAfHelper : public AfHelper{
		public:
			UnixAfHelper(const seastar::sstring&ip, uint16_t port){
				_address = seastar::socket_address{seastar::unix_domain_addr{ip}};
				seastar::listen_options lo;
				lo.reuse_address = true;
//				lo.set_fixed_cpu(seastar::this_shard_id());
				_listener = seastar::listen(getAddress(), lo);
				fmt::print("created UNIX listener {}\n",ip);
			}
			~UnixAfHelper(){
			}
			seastar::future<seastar::connected_socket> connect() override{
				throw (std::logic_error("connect is not implemented"));
			}
			seastar::future<seastar::accept_result> accept() override{
				return _listener.accept();
			}
			seastar::socket_address getAddress() override {
				return _address;
			}
		private:
			seastar::socket_address _address;
			seastar::server_socket _listener;
	};
}
