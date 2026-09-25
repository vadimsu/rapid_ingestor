
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_listener.hh"
#include "rapid_ingestor_protocol.hh"

namespace bpo = boost::program_options;

int main(int argc, char **argv){
	seastar::app_template app;
        app.add_options()
                        ("config", bpo::value<seastar::sstring>()->default_value({}), "path to config file");
        return app.run_deprecated(argc, argv, [&app]{
                auto& args = app.configuration();
                //GdnsFileSync::filesyncapp = new distributed<GdnsFileSync::gdnsfilesync_app>();
			seastar::lw_shared_ptr<RapidIngestor::Config> config = seastar::make_lw_shared<RapidIngestor::Config>(args["config"].as<seastar::sstring>());
		seastar::engine().at_exit([] {
			fmt::print("at_exist\n");
                        return seastar::make_ready_future<>();
                });
		fmt::print("main\n");
		return config->read().then([config]{
				const auto& sources = config->getSources();
				std::vector<seastar::future<>> futs;
				std::vector<seastar::lw_shared_ptr<RapidIngestor::Listener>> listeners;
				for (const auto& src : sources){
					if (src.protocol == "TCP"){
						auto tcpAfHelper = std::make_shared<RapidIngestor::TcpAfHelper>(src.ipaddr, src.port);
						auto listener = seastar::make_lw_shared<RapidIngestor::Listener>(tcpAfHelper);
						auto fut = listener->listen();
						listeners.push_back(listener);
						futs.push_back(std::move(fut));
						fmt::print("created listener {} {}\n",src.ipaddr,src.port);
					}else if (src.protocol == "TLS"){
					}else{
						fmt::print("unknown protocol {}\n",src.protocol);
					}
				}
				fmt::print("{}\n",futs.size());
				return when_all(futs.begin(),futs.end()).then([listeners] (auto futs){
						fmt::print("{} {}\n",__FILE__,__LINE__);
						return seastar::make_ready_future<>();
					});
			});
        });
}
