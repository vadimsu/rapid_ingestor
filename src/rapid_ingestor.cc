
#include<chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_unix_af_helper.hh"
#include "rapid_ingestor_listener.hh"
#include "rapid_ingestor_protocol.hh"

namespace bpo = boost::program_options;

auto listeners = new seastar::sharded<RapidIngestor::Listener>();
seastar::timer<> statsTimer = seastar::timer<>();
RapidIngestor::RapidIngestorStats gMessagesProcessed;

seastar::future<RapidIngestor::RapidIngestorStats> get_all_stats(){
	return listeners->map_reduce(seastar::adder<RapidIngestor::RapidIngestorStats>(),&RapidIngestor::Listener::getStats);
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
					}else if (src->protocol == "UNIX"){
						fmt::print("removing unix domain socket file {}\n",src->ipaddr);
						seastar::remove_file(fmt::format("{}",src->ipaddr));
						break;
					}else{
						fmt::print("unknown protocol {}\n",src->protocol);
					}
				}
				if (src != sources.end()){
					const auto& sinks = config->getSinks();
					auto sinkIt = sinks.begin();
					RapidIngestor::Sink sink = (sinkIt != sinks.end()) ? *sinkIt : RapidIngestor::Sink{};
					return listeners->start().then([source=*src, sink] {
						statsTimer.set_callback([]{
							get_all_stats().then([](RapidIngestor::RapidIngestorStats stats){
								gMessagesProcessed += stats;
								fmt::print("Total messages {} bytes {}\n",stats.messagesParsed, stats.bytesProcessed);
							});
						});
						statsTimer.arm_periodic(std::chrono::seconds(1));
						return listeners->invoke_on_all(&RapidIngestor::Listener::listen, source.ipaddr, source.port, sink);
					});
				}
				return seastar::make_ready_future<>();
			});
		});
}
