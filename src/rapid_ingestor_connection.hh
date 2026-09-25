#pragma once

#include <vector>
#include <seastar/core/seastar.hh>
#include "seastar/net/api.hh"
#include <seastar/net/inet_address.hh>
#include <seastar/core/when_all.hh>

namespace RapidIngestor{

	class Connection : public seastar::enable_lw_shared_from_this<Connection> {
		public:
			Connection(seastar::connected_socket fd, seastar::socket_address addr): _fd(std::move(fd)), _addr(addr), _in(_fd.input()), _out(_fd.output()) {
//				seastar::input_stream_options stream_opts;
//				stream_opts.buffer_size = 65536;

//				seastar::input_stream<char> in = seastar::make_file_input_stream(std::move(fd), stream_opts);
			}
			~Connection(){
//				fmt::print("{} {}\n",__func__,__LINE__);
			}
			seastar::future<seastar::temporary_buffer<char>> receive(){
				if (_in.eof()){
					co_return seastar::temporary_buffer<char>();
				}
				co_return co_await _in.read();
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
