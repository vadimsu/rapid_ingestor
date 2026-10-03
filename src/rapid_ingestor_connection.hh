#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include <seastar/core/when_all.hh>
#define _GNU_SOURCE
#include <netinet/tcp.h>
#include <linux/net_tstamp.h>

namespace RapidIngestor{

	class Connection : public seastar::enable_lw_shared_from_this<Connection> {
		public:
			Connection(seastar::connected_socket fd, seastar::socket_address addr): _fd(std::move(fd)), _addr(addr), _in(_fd.input([]{
																	seastar::connected_socket_input_stream_config csisc;
																	csisc.buffer_size     = 128 * 1024;
																	csisc.min_buffer_size = 128 * 1024;
																	csisc.max_buffer_size = 128 * 1024;
																	return csisc;
																	}())), _out(_fd.output()) {
//				int size = 1024*1024*100;
//				int size = 65535;
//				_fd.set_sockopt(SOL_SOCKET, SO_RCVBUF, (const void*) &size, sizeof(size));
//				_fd.set_sockopt(SOL_SOCKET, SO_SNDBUF, (const void*) &size, sizeof(size));
				int opt = 4096;
				_fd.set_sockopt(SOL_SOCKET, SO_RCVLOWAT, (const void*) &opt, sizeof(opt));
				opt = 1;
				_fd.set_sockopt(IPPROTO_TCP, TCP_QUICKACK, (const void*) &opt, sizeof(opt));
				opt = 1;
				_fd.set_sockopt(IPPROTO_TCP, TCP_NODELAY, (const void*) &opt, sizeof(opt));
				int flags = SOF_TIMESTAMPING_RX_HARDWARE | SOF_TIMESTAMPING_RX_SOFTWARE | SOF_TIMESTAMPING_SOFTWARE;

				_fd.set_sockopt(SOL_SOCKET, SO_TIMESTAMPING, &flags, sizeof(flags));
			}
			~Connection(){
//				fmt::print("{} {}\n",__func__,__LINE__);
			}
			seastar::future<seastar::temporary_buffer<char>> receive(){
				if (_in.eof()){
					co_return seastar::temporary_buffer<char>();
				}
				auto buf = co_await  _in.read();
				co_return buf;
			}
			bool isAlive() { return !_in.eof(); }
			seastar::future<> stop() {
				auto in_fut = _in.close();
				auto out_fut = _out.close();
				std::vector<seastar::future<>> futs;
				futs.push_back(std::move(in_fut));
				futs.push_back(std::move(out_fut));
				return seastar::when_all(futs.begin(), futs.end()).then([this] (auto fs){
					return seastar::make_ready_future<>();
				});
			}
		private:
			seastar::connected_socket _fd;
			seastar::socket_address _addr;
			seastar::input_stream<char> _in;
			seastar::output_stream<char> _out;
	};
}
