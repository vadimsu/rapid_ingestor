
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_listener.hh"
#include "rapid_ingestor_protocol.hh"

namespace bpo = boost::program_options;

auto handlers = new seastar::sharded<RapidIngestor::ShardedHandler>();

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
				return handlers->start().then([config] {
					const auto& sources = config->getSources();
					for (const auto& src : sources){
						if (src.protocol == "TCP"){
							auto listener = new seastar::sharded<RapidIngestor::Listener>();
							return listener->start().then([listener, src] {
									return listener->invoke_on(0, &RapidIngestor::Listener::listen, src.ipaddr, src.port, std::ref(*handlers));
							});
							fmt::print("created listener {} {}\n",src.ipaddr,src.port);
						}else if (src.protocol == "TLS"){
							fmt::print("TLS listeners are not supported yet\n");
						}else{
							fmt::print("unknown protocol {}\n",src.protocol);
						}
					}
					return seastar::make_ready_future<>();
				});
			});
        });
}
