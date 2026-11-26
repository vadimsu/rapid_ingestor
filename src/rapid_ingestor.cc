
#include<chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include "rapid_ingestor_config.hh"
#include "rapid_ingestor_tcp_af_helper.hh"
#include "rapid_ingestor_unix_af_helper.hh"
#include "rapid_ingestor_listener.hh"

namespace bpo = boost::program_options;

//auto listeners = new seastar::sharded<RapidIngestor::Listener>();
std::vector<seastar::sharded<RapidIngestor::Listener>*> listeners;
//seastar::timer<> statsTimer = seastar::timer<>();
std::vector<seastar::timer<>> statsTimers;
RapidIngestor::RapidIngestorStats gMessagesProcessed;

seastar::future<RapidIngestor::RapidIngestorStats> get_all_stats(seastar::sharded<RapidIngestor::Listener>* listener){
	return listener->map_reduce(seastar::adder<RapidIngestor::RapidIngestorStats>(),&RapidIngestor::Listener::getStats);
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
				std::vector<seastar::future<>> futs;
				for (;src != sources.end();src++){
					fmt::print("created listener {} {}\n",src->ipaddr,src->port);
					auto listener = new seastar::sharded<RapidIngestor::Listener>();
					listeners.push_back(listener);
					statsTimers.push_back(seastar::timer<>());
					statsTimers[statsTimers.size()-1].set_callback([listener, source=*src]{
						get_all_stats(listener).then([source](RapidIngestor::RapidIngestorStats stats){
							gMessagesProcessed += stats;
							fmt::print("Listener {}:{} ({}) Total messages {} bytes {} rows inserted {} unparsed {} fatal errors {}\n",
								source.ipaddr, source.port, source.protocol, stats.messagesParsed,
								stats.bytesProcessed, stats.rowsInserted, stats.unparsedBufferSize,stats.fatalErrors);
						});
					});
					statsTimers[statsTimers.size()-1].arm_periodic(std::chrono::seconds(1));
					auto fut = listener->start().then([source=*src, config, listener] {
						const auto& sinks = config->getSinks();
						const auto sinkIt = sinks.begin();
						const RapidIngestor::Sink& sink = *sinkIt;
						fmt::print("{} {} {}\n",__func__,__LINE__,sink.mode);
						return listener->invoke_on_all(&RapidIngestor::Listener::listen, source.ipaddr, source.port, sink, source.protocol).handle_exception([](std::exception_ptr ep){
							fmt::print("{} {} {}\n",__FILE__,__LINE__,ep);
							return seastar::make_ready_future<>();
						});
					});
					futs.push_back(std::move(fut));
				}
				return seastar::when_all(futs.begin(), futs.end()).then([] (auto futs){
					return seastar::make_ready_future<>();
				});
			});
		});
}
