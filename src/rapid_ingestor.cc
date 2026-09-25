
#include<chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_listener.hh"
#include "rapid_ingestor_protocol.hh"

namespace bpo = boost::program_options;

auto listeners = new seastar::sharded<RapidIngestor::Listener>();
seastar::timer<> statsTimer = seastar::timer<>();
uint64_t gMessagesProcessed = 0;

seastar::future<uint64_t> get_all_stats(){
	return listeners->map_reduce(seastar::adder<uint64_t>(),&RapidIngestor::Listener::getStats);
}

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
				auto src = sources.begin();
				for (;src != sources.end();src++){
					if (src->protocol == "TCP"){
						break;
						fmt::print("created listener {} {}\n",src->ipaddr,src->port);
					}else if (src->protocol == "TLS"){
						fmt::print("TLS listeners are not supported yet\n");
					}else{
						fmt::print("unknown protocol {}\n",src->protocol);
					}
				}
				if (src != sources.end()){
					return listeners->start().then([source=*src] {
						statsTimer.set_callback([]{
							get_all_stats().then([](uint64_t msgs){
								gMessagesProcessed += msgs;
								fmt::print("Total messages {}\n",gMessagesProcessed);
							});
						});
						statsTimer.arm_periodic(std::chrono::seconds(5));
						return listeners->invoke_on_all(&RapidIngestor::Listener::listen, source.ipaddr, source.port);
					});
				}
				return seastar::make_ready_future<>();
			});
		});
}
