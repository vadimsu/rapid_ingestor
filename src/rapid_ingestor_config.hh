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
	// One explicit mapping from a StackLogFrame field to a ClickHouse column,
	// used by Sink::columns (multi-column / "superset schema" mode - see
	// rapid_ingestor_clickhouse_sink.hh). `source` is the field name looked up
	// in the incoming record; `target_column` is the ClickHouse column name
	// it's written to (defaults to `source` when omitted, i.e. no aliasing).
	// `type` is "string" | "int64" | "bool". `encoding` ("text" | "json")
	// only applies to "string" columns and controls how a non-string field
	// value is coerced into text; defaults to "json".
	struct SinkColumn{
		seastar::sstring source;
		seastar::sstring target_column;
		seastar::sstring type = "string";
		seastar::sstring encoding;
	};
	struct Sink{
		seastar::sstring database_type = "clickhouse";
		seastar::sstring database_table;
		seastar::sstring database_connection;
		// ClickHouse native-protocol connection + record serialization mode
		// ("raw_text" | "tag_map" | "dynamic" | "json"); see
		// rapid_ingestor_clickhouse_sink.hh for what each mode does.
		// Only used when `columns` below is empty (single whole-record column).
		seastar::sstring mode = "raw_text";
		seastar::sstring host = "127.0.0.1";
		uint16_t port = 9000;
		seastar::sstring dbname = "default";
		seastar::sstring username = "default";
		seastar::sstring password;
		size_t max_batch_bytes;
		size_t backpressure_pause_bytes;
		size_t backpressure_resume_bytes;
		// Single-column (whole-record) mode: ClickHouse column name the
		// serialized record is written to (replaces the previously hardcoded
		// "payload"), and its text encoding ("text" | "json"; empty = derive
		// from `mode`, i.e. raw_text -> text, everything else -> json).
		seastar::sstring target_column = "payload";
		seastar::sstring encoding = "text";
		// Explicit column schema: when non-empty, switches the sink to
		// multi-column mode - the input record's fields are a superset of
		// this schema, one wire column is produced per entry below instead of
		// a single serialized `target_column`.
		std::vector<SinkColumn> columns;
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
					auto target_column_it = it->find("target_column");
					if (target_column_it != it->end()){
						std::string target_column = to_string(*target_column_it);
						sink.target_column = target_column.substr(1, target_column.size() - 2);
					}
					auto encoding_it = it->find("encoding");
					if (encoding_it != it->end()){
						std::string encoding = to_string(*encoding_it);
						sink.encoding = encoding.substr(1, encoding.size() - 2);
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
					auto max_batch_bytes_it = it->find("max_batch_bytes");
					if (max_batch_bytes_it != it->end()){
						sink.max_batch_bytes = max_batch_bytes_it->get<size_t>();
					}
					auto backpressure_pause_bytes_it = it->find("backpressure_pause_bytes");
					if (backpressure_pause_bytes_it != it->end()){
						sink.backpressure_pause_bytes = backpressure_pause_bytes_it->get<size_t>();
					}
					auto backpressure_resume_bytes_it = it->find("backpressure_resume_bytes");
					if (backpressure_resume_bytes_it != it->end()){
						sink.backpressure_resume_bytes = backpressure_resume_bytes_it->get<size_t>();
					}
					auto columns_it = it->find("columns");
					if (columns_it != it->end() && columns_it->is_array()){
						for (auto col_it = columns_it->begin(); col_it != columns_it->end(); col_it++){
							SinkColumn col;
							auto source_it = col_it->find("source");
							if (source_it != col_it->end()){
								std::string source = to_string(*source_it);
								col.source = source.substr(1, source.size() - 2);
							}
							auto col_target_it = col_it->find("target_column");
							if (col_target_it != col_it->end()){
								std::string col_target = to_string(*col_target_it);
								col.target_column = col_target.substr(1, col_target.size() - 2);
							} else {
								col.target_column = col.source;
							}
							auto type_it = col_it->find("type");
							if (type_it != col_it->end()){
								std::string type = to_string(*type_it);
								col.type = type.substr(1, type.size() - 2);
							}
							auto col_encoding_it = col_it->find("encoding");
							if (col_encoding_it != col_it->end()){
								std::string col_encoding = to_string(*col_encoding_it);
								col.encoding = col_encoding.substr(1, col_encoding.size() - 2);
							}
							fmt::print("Sink column source {} target_column {} type {} encoding {}\n", col.source, col.target_column, col.type, col.encoding);
							sink.columns.push_back(col);
						}
					}

					fmt::print("Sink DB type {} table {} connection {} mode {} host {} port {} target_column {} encoding {} columns {} max_batch_bytes {} backpressure_pause_bytes {} backpressure_resume_bytes {}\n",db_type, db_table,db_connection, sink.mode, sink.host, sink.port, sink.target_column, sink.encoding, sink.columns.size(), sink.max_batch_bytes, sink.backpressure_pause_bytes, sink.backpressure_resume_bytes);
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
