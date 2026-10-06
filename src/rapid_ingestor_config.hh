#pragma once

#include <vector>
#include "nlohmann/json.hpp"
#include "seastar/core/file.hh"
#include "seastar/core/thread.hh"

namespace fs = std::filesystem;

namespace RapidIngestor{
	struct Source{
		seastar::sstring protocol;
		seastar::sstring ipaddr;
		seastar::sstring key;
		seastar::sstring cert;
		seastar::sstring trusted;
		uint16_t port;
	};
	class Sources {
		public:
			using iterator = std::vector<Source>::iterator;
			using const_iterator = std::vector<Source>::const_iterator;

			Sources(){fmt::print("_source {}\n",_sources.size());}
			~Sources(){fmt::print("{} {}\n",__func__,__LINE__);}
			void addSource(const seastar::sstring& protocol, const seastar::sstring& ipaddr, const seastar::sstring& key, const seastar::sstring& cert, const seastar::sstring& trusted, uint16_t port){
				fmt::print("_source {}\n",_sources.size());
				_sources.push_back(Source{protocol: protocol, ipaddr: ipaddr, key: key, cert: cert, trusted: trusted, port: port});
				fmt::print("_source {}\n",_sources.size());
			}

			iterator begin() { return _sources.begin(); }
			iterator end() { return _sources.end(); }

			const_iterator begin() const { return _sources.begin(); }
			const_iterator end() const { return _sources.end(); }
		private:
			std::vector<Source> _sources;
	};
	struct Sink{
		seastar::sstring database_type;
		seastar::sstring database_table;
		seastar::sstring database_connection;
		// ClickHouse native-protocol connection + record serialization mode
		// ("raw_text" | "tag_map" | "dynamic" | "json"); see
		// rapid_ingestor_clickhouse_sink.hh for what each mode does.
		seastar::sstring mode = "raw_text";
		seastar::sstring host = "127.0.0.1";
		uint16_t port = 9000;
		seastar::sstring dbname = "default";
		seastar::sstring username = "default";
		seastar::sstring password;
	};
	class Sinks{
		public:
			using iterator = std::vector<Sink>::iterator;
			using const_iterator = std::vector<Sink>::const_iterator;

			Sinks(){fmt::print("_sinks {}\n",_sinks.size());}
			~Sinks(){fmt::print("{} {}\n",__func__,__LINE__);}
			void addSink(const Sink& sink){
				fmt::print("_sinks {}\n",_sinks.size());
				_sinks.push_back(sink);
				fmt::print("_sinks {}\n",_sinks.size());
			}
			iterator begin() { return _sinks.begin(); }
			iterator end() { return _sinks.end(); }

			const_iterator begin() const { return _sinks.begin(); }
			const_iterator end() const { return _sinks.end(); }
		private:
			std::vector<Sink> _sinks;
	};
	class Config : public seastar::enable_lw_shared_from_this<Config> {
		public:
			Config(const seastar::sstring& json_file): _json_file(json_file){}
			~Config(){fmt::print("{} {}\n",__func__,__LINE__);}
			seastar::future<> read(){
				 fs::path config_filename(_json_file);
				fmt::print("reading config\n");
			        return seastar::open_file_dma(config_filename.string(), seastar::open_flags::ro).then([this,  this_ptr = this->shared_from_this()] (seastar::file f) {
                        		return do_with(std::move(f), [this] (seastar::file& f) {
                                        	return f.size().then([this, &f] (size_t s) {
                                                        return f.dma_read_exactly<char>(0, s);
                                        });
	                        }).then([this, this_ptr = this->shared_from_this()] (seastar::temporary_buffer<char> tb) {
					nlohmann::json	jsonPayload;
					seastar::sstring output(tb.get());
                        	        try {
                                	        jsonPayload = nlohmann::json::parse(output);
						process_json(jsonPayload);
        	                        } catch(std::exception& exc){
						std::cout<<"Exception "<<exc.what()<<std::endl;
                        	        }
        	                        return seastar::make_ready_future<>();
                	                });
                        	}).finally([this]{
                                	/*_periodicStatsTimer.set_callback([this]{
                                        	(void)periodicStats().then([this] (auto result){
                                                	fmt::print("Stats: {}\n",result);
	                                                _periodicStatsTimer.rearm(std::chrono::steady_clock::now() + std::chrono::seconds(30));
        	                                });
                	                });
                        	        _periodicStatsTimer.rearm(std::chrono::steady_clock::now() + std::chrono::seconds(120));*/
					return seastar::make_ready_future<>();
	                        });
			}
			const Sources& getSources(){ return _sources; }
			const Sinks& getSinks() { return _sinks; }	
		private:
			void process_sources(nlohmann::json& jsonPayload){
				auto sources_it = jsonPayload.find("sources");
			        if (sources_it == jsonPayload.end()) {
                			return;
		        	}
			        for (auto it = sources_it->begin(); it != sources_it->end(); it++){
					std::string protocol, port, key, cert, trusted, ipaddr;
        	        		auto protocol_it = it->find("protocol");
					if (protocol_it != it->end()){
				                protocol = to_string(*protocol_it);
        	        			protocol = protocol.substr(1, protocol.size() - 2);
					}
					if (protocol == "UNIX"){
						auto path_it = it->find("path");
						if (path_it != it->end()){
							ipaddr = to_string(*path_it);
							ipaddr = ipaddr.substr(1, ipaddr.size() - 2);
							port = "0";//dummy
						}
					}else{
						if (protocol == "TLS"){
						}
						auto ip_it = it->find("ip");
						if (ip_it != it->end()){
							ipaddr = to_string(*ip_it);
							ipaddr = ipaddr.substr(1, ipaddr.size() - 2);
						}
						auto port_it = it->find("port");
						if (port_it != it->end()){
							port = to_string(*port_it);
							port = port.substr(1, port.size() - 2);
						}
					}
					fmt::print("Source protocol {} ip {} key {} cert {} trusted {} port {}\n",protocol, ipaddr, key,cert,trusted,port);
					_sources.addSource(protocol, ipaddr, key, cert, trusted, std::stoi(port));
			                /*for (unsigned core = 0; core < smp::count; core++) {
                		        	(void)loggingController<AppFlavor...>->invoke_on(core, &LoggingController<AppFlavor...>::setEventEnabled, *ev, enabled);
	                		}*/
	        		}
			}
			void process_sinks(nlohmann::json& jsonPayload){
				auto sinks_it = jsonPayload.find("sinks");
			        if (sinks_it == jsonPayload.end()) {
                			return;
		        	}
			        for (auto it = sinks_it->begin(); it != sinks_it->end(); it++){
					Sink sink;
					std::string db_type, db_table, db_connection;
        	        		auto db_type_it = it->find("database_type");
					if (db_type_it != it->end()){
						db_type = to_string(*db_type_it);
						db_type = db_type.substr(1, db_type.size() - 2);
					}
					auto db_table_it = it->find("database_table");
					if (db_table_it != it->end()){
				                db_table = to_string(*db_table_it);
        	        			db_table = db_table.substr(1, db_table.size() - 2);
					}
					auto db_connection_it = it->find("database_connection");
					if (db_connection_it != it->end()){
				                db_connection = to_string(*db_connection_it);
        	        			db_connection = db_connection.substr(1, db_connection.size() - 2);
					}
					sink.database_type = db_type;
					sink.database_table = db_table;
					sink.database_connection = db_connection;

					auto mode_it = it->find("mode");
					if (mode_it != it->end()){
						std::string mode = to_string(*mode_it);
						sink.mode = mode.substr(1, mode.size() - 2);
					}
					auto host_it = it->find("host");
					if (host_it != it->end()){
						std::string host = to_string(*host_it);
						sink.host = host.substr(1, host.size() - 2);
					}
					auto port_it = it->find("port");
					if (port_it != it->end()){
						sink.port = static_cast<uint16_t>(port_it->is_string() ?
							std::stoi(to_string(*port_it).substr(1, to_string(*port_it).size() - 2)) :
							port_it->get<int>());
					}
					auto dbname_it = it->find("dbname");
					if (dbname_it != it->end()){
						std::string dbname = to_string(*dbname_it);
						sink.dbname = dbname.substr(1, dbname.size() - 2);
					}
					auto username_it = it->find("username");
					if (username_it != it->end()){
						std::string username = to_string(*username_it);
						sink.username = username.substr(1, username.size() - 2);
					}
					auto password_it = it->find("password");
					if (password_it != it->end()){
						std::string password = to_string(*password_it);
						sink.password = password.substr(1, password.size() - 2);
					}

					fmt::print("Sink DB type {} table {} connection {} mode {} host {} port {}\n",db_type, db_table,db_connection, sink.mode, sink.host, sink.port);
					_sinks.addSink(sink);
			                /*for (unsigned core = 0; core < smp::count; core++) {
                		        	(void)loggingController<AppFlavor...>->invoke_on(core, &LoggingController<AppFlavor...>::setEventEnabled, *ev, enabled);
	                		}*/
	        		}
			}
			void process_json(nlohmann::json& jsonPayload){
			        process_sources(jsonPayload);
				process_sinks(jsonPayload);
			}
			seastar::sstring _json_file;
			Sources _sources;
			Sinks _sinks;
	};
}
