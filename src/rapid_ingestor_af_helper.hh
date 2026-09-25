#pragma once

#include <vector>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>


namespace RapidIngestor{

	class AfHelper {
		public:
			virtual seastar::future<seastar::connected_socket> connect() = 0;
			virtual seastar::future<seastar::accept_result> accept() = 0;
			virtual seastar::socket_address getAddress() {
				throw (std::logic_error("getAddress is not implemented"));
			}
		private:
	};
}
