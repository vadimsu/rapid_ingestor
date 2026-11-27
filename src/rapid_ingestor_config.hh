#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <exception>
#include <functional>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include "nlohmann/json.hpp"
#include "seastar/core/file.hh"
#include "seastar/core/thread.hh"

namespace fs = std::filesystem;

namespace RapidIngestor{
	struct PipelineComponent {
		seastar::sstring id;
		seastar::sstring type;
		std::vector<seastar::sstring> inputs;
	};

	class PipelineGraph {
		public:
			const std::vector<PipelineComponent>& components() const { return _components; }
			const std::vector<seastar::sstring>& topologicalOrder() const { return _topological_order; }
			bool empty() const { return _components.empty(); }

			void add(PipelineComponent component) {
				if (component.id.empty()) {
					throw std::invalid_argument("pipeline component id must not be empty");
				}
				if (!_by_id.emplace(component.id, _components.size()).second) {
					throw std::invalid_argument(
						fmt::format("duplicate pipeline component '{}'", component.id));
				}
				_components.push_back(std::move(component));
			}

			void validate() {
				enum class Visit : uint8_t { Unvisited, Visiting, Visited };
				std::vector<Visit> state(_components.size(), Visit::Unvisited);
				_topological_order.clear();
				std::function<void(size_t)> visit = [&](size_t index) {
					if (state[index] == Visit::Visiting) {
						throw std::invalid_argument("pipeline graph contains a cycle");
					}
					if (state[index] == Visit::Visited) {
						return;
					}
					state[index] = Visit::Visiting;
					for (const auto& input : _components[index].inputs) {
						auto it = _by_id.find(input);
						if (it == _by_id.end()) {
							auto separator = input.find('.');
							if (separator != std::string::npos) {
								auto source_id = input.substr(0, separator);
								auto source_it = _by_id.find(source_id);
								if (source_it != _by_id.end() &&
									(_components[source_it->second].type == "opentelemetry" ||
									 _components[source_it->second].type == "otel")) {
									auto port = input.substr(separator + 1);
									if (port != "traces" && port != "metrics" && port != "logs") {
										throw std::invalid_argument(fmt::format(
											"source '{}' has unsupported output port '{}'",
											source_id, port));
									}
									continue;
								}
							}
						}
						if (it == _by_id.end()) {
							throw std::invalid_argument(fmt::format(
								"component '{}' references unknown input '{}'",
								_components[index].id, input));
						}
						if (_components[it->second].type == "clickhouse") {
							throw std::invalid_argument(fmt::format(
								"component '{}' cannot use sink '{}' as an input",
								_components[index].id, input));
						}
						visit(it->second);
					}
					state[index] = Visit::Visited;
					_topological_order.push_back(_components[index].id);
				};
				std::vector<size_t> order(_components.size());
				std::iota(order.begin(), order.end(), 0);
				std::sort(order.begin(), order.end(), [&](size_t left, size_t right) {
					return _components[left].id < _components[right].id;
				});
				for (auto index : order) {
					visit(index);
				}
			}

		private:
			std::vector<PipelineComponent> _components;
			std::unordered_map<std::string, size_t> _by_id;
			std::vector<seastar::sstring> _topological_order;
	};

	struct Source{
		seastar::sstring id;
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
			void addSource(const seastar::sstring& protocol, const seastar::sstring& ipaddr, const seastar::sstring& key, const seastar::sstring& cert, const seastar::sstring& trusted, uint16_t port, const seastar::sstring& id = {}){
				fmt::print("_source {}\n",_sources.size());
				_sources.push_back(Source{id: id, protocol: protocol, ipaddr: ipaddr, key: key, cert: cert, trusted: trusted, port: port});
				fmt::print("_source {}\n",_sources.size());
			}

			iterator begin() { return _sources.begin(); }
			iterator end() { return _sources.end(); }

			const_iterator begin() const { return _sources.begin(); }
			const_iterator end() const { return _sources.end(); }
			size_t size() const { return _sources.size(); }
			const Source& operator[](size_t index) const { return _sources[index]; }
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
		seastar::sstring id;
		std::vector<seastar::sstring> inputs;
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
		size_t max_batch_bytes = 10ull * 1024 * 1024;
		size_t backpressure_pause_bytes = 8ull * 1024 * 1024;
		size_t backpressure_resume_bytes = 4ull * 1024 * 1024;
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
			size_t size() const { return _sinks.size(); }
			Sink& operator[](size_t index) { return _sinks[index]; }
			const Sink& operator[](size_t index) const { return _sinks[index]; }
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
                        	        } catch(...) {
						return seastar::make_exception_future<>(std::current_exception());
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
			const PipelineGraph& getPipeline() const { return _pipeline; }
		private:
		        	static std::string string_value(
		        		const nlohmann::json& object, const char* key, std::string fallback = {}) {
		        		auto it = object.find(key);
		        		if (it == object.end() || it->is_null()) {
		        			return fallback;
		        		}
		        		if (!it->is_string()) {
		        			throw std::invalid_argument(fmt::format(
		        				"configuration field '{}' must be a string", key));
		        		}
		        		return it->get<std::string>();
		        	}

		        	static uint16_t port_value(const nlohmann::json& object, const char* key,
		        		uint16_t fallback = 0) {
		        		auto it = object.find(key);
		        		if (it == object.end() || it->is_null()) {
		        			return fallback;
		        		}
		        		if (it->is_number_unsigned() || it->is_number_integer()) {
		        			auto value = it->get<unsigned long long>();
		        			if (value > UINT16_MAX) {
		        				throw std::invalid_argument(fmt::format(
		        					"configuration field '{}' is outside the port range", key));
		        			}
		        			return static_cast<uint16_t>(value);
		        		}
		        		if (it->is_string()) {
		        			auto value = std::stoul(it->get<std::string>());
		        			if (value > UINT16_MAX) {
		        				throw std::invalid_argument(fmt::format(
		        					"configuration field '{}' is outside the port range", key));
		        			}
		        			return static_cast<uint16_t>(value);
		        		}
		        		throw std::invalid_argument(fmt::format(
		        			"configuration field '{}' must be a port number", key));
		        	}

		        	static std::pair<std::string, uint16_t> address_value(
		        		const nlohmann::json& object, const char* nested_key) {
		        		std::string address;
		        		auto nested = object.find(nested_key);
		        		if (nested != object.end() && nested->is_object()) {
		        			address = string_value(*nested, "address");
		        		}
		        		if (address.empty()) {
		        			address = string_value(object, "address");
		        		}
		        		if (address.empty()) {
		        			return {string_value(object, "ip", "127.0.0.1"),
		        				port_value(object, "port")};
		        		}
		        		auto colon = address.rfind(':');
		        		if (colon == std::string::npos) {
		        			throw std::invalid_argument(fmt::format(
		        				"address '{}' must include a port", address));
		        		}
		        		std::string host = address.substr(0, colon);
		        		if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
		        			host = host.substr(1, host.size() - 2);
		        		}
		        		return {host, static_cast<uint16_t>(std::stoul(address.substr(colon + 1)))};
		        	}

		        	static std::vector<seastar::sstring> inputs_value(
		        		const nlohmann::json& object) {
		        		std::vector<seastar::sstring> inputs;
		        		auto it = object.find("inputs");
		        		if (it == object.end()) {
		        			return inputs;
		        		}
		        		if (!it->is_array()) {
		        			throw std::invalid_argument("component 'inputs' must be an array");
		        		}
		        		for (const auto& input : *it) {
		        			if (!input.is_string()) {
		        				throw std::invalid_argument("component input must be a string");
		        			}
		        			inputs.emplace_back(input.get<std::string>());
		        		}
		        		return inputs;
		        	}

		        	void process_pipeline_sources(const nlohmann::json& sources) {
		        		if (!sources.is_object()) {
		        			throw std::invalid_argument("named 'sources' must be an object");
		        		}
		        		for (const auto& [id, value] : sources.items()) {
		        			if (!value.is_object()) {
		        				throw std::invalid_argument(fmt::format(
		        					"source '{}' must be an object", id));
		        			}
		        			auto type = string_value(value, "type");
		        			auto protocol = string_value(value, "protocol", type);
		        			std::transform(protocol.begin(), protocol.end(), protocol.begin(), ::tolower);
		        			if (protocol == "opentelemetry" || protocol == "otel") {
		        				protocol = "otel";
		        				type = "opentelemetry";
		        			}
		        			if (protocol == "fluent" || protocol == "fluent_forward" ||
		        				protocol == "tcp") protocol = "forward";
		        			if (protocol != "otel" && protocol != "forward") {
		        				throw std::invalid_argument(fmt::format(
		        					"source '{}' has unsupported protocol '{}'", id, protocol));
		        			}
		        			auto address = address_value(value, protocol == "otel" ? "grpc" : "tcp");
		        			_sources.addSource(protocol, address.first, string_value(value, "key"),
		        				string_value(value, "certificate"), string_value(value, "trusted"),
		        				address.second, id);
		        			// Store the canonical runtime protocol in the graph. The
		        			// user-facing aliases (fluent_forward, fluent, tcp, etc.)
		        			// are normalized above.
		        			_pipeline.add({id, protocol, {}});
		        		}
		        	}

		        	static Sink parse_named_sink(const std::string& id,
		        		const nlohmann::json& value) {
		        		if (!value.is_object()) {
		        			throw std::invalid_argument(fmt::format(
		        				"sink '{}' must be an object", id));
		        		}
		        		Sink sink;
		        		sink.id = id;
		        		sink.inputs = inputs_value(value);
		        		sink.database_type = string_value(value, "database_type",
		        			string_value(value, "type", "clickhouse"));
		        		sink.database_table = string_value(value, "database_table",
		        			string_value(value, "table"));
		        		sink.database_connection = string_value(value, "database_connection");
		        		sink.mode = string_value(value, "mode", sink.mode);
		        		sink.target_column = string_value(value, "target_column", sink.target_column);
		        		sink.encoding = string_value(value, "encoding", sink.encoding);
		        		sink.host = string_value(value, "host", sink.host);
		        		sink.port = port_value(value, "port", sink.port);
		        		sink.dbname = string_value(value, "dbname",
		        			string_value(value, "database", sink.dbname));
		        		sink.username = string_value(value, "username",
		        			string_value(value, "user", sink.username));
		        		sink.password = string_value(value, "password", sink.password);
		        		sink.max_batch_bytes = value.value("max_batch_bytes", sink.max_batch_bytes);
		        		sink.backpressure_pause_bytes =
		        			value.value("backpressure_pause_bytes", sink.backpressure_pause_bytes);
		        		sink.backpressure_resume_bytes =
		        			value.value("backpressure_resume_bytes", sink.backpressure_resume_bytes);
		        		return sink;
		        	}

		        	void process_pipeline_sinks(const nlohmann::json& sinks) {
		        		if (!sinks.is_object()) {
		        			throw std::invalid_argument("named 'sinks' must be an object");
		        		}
		        		for (const auto& [id, value] : sinks.items()) {
		        			auto sink = parse_named_sink(id, value);
		        			if (sink.database_type != "clickhouse") {
		        				throw std::invalid_argument(fmt::format(
		        					"sink '{}' has unsupported type '{}'", id, sink.database_type));
		        			}
		        			if (sink.inputs.empty()) {
		        				throw std::invalid_argument(fmt::format(
		        					"sink '{}' must have at least one input", id));
		        			}
		        			_sinks.addSink(sink);
		        			_pipeline.add({id, string_value(value, "type", "clickhouse"),
		        				sink.inputs});
		        		}
		        	}

		        	void process_pipeline_transforms(const nlohmann::json& transforms) {
		        		if (!transforms.is_object()) {
		        			throw std::invalid_argument("named 'transforms' must be an object");
		        		}
		        		for (const auto& [id, value] : transforms.items()) {
		        			if (!value.is_object()) {
		        				throw std::invalid_argument(fmt::format(
		        					"transform '{}' must be an object", id));
		        			}
		        			auto type = string_value(value, "type");
		        			if (type.empty()) {
		        				throw std::invalid_argument(fmt::format(
		        					"transform '{}' must have a type", id));
		        			}
		        			if (type != "filter" && type != "remap" && type != "route" &&
		        				type != "rename") {
		        				throw std::invalid_argument(fmt::format(
		        					"transform '{}' has unsupported type '{}'", id, type));
		        			}
		        			_pipeline.add({id, type, inputs_value(value)});
		        		}
		        	}
			void process_sources(nlohmann::json& jsonPayload){
				auto sources_it = jsonPayload.find("sources");
			        if (sources_it == jsonPayload.end()) {
                			return;
		        	}
			        for (auto it = sources_it->begin(); it != sources_it->end(); it++){
					std::string protocol, port = "0", key, cert, trusted, ipaddr, path;
        	        		auto protocol_it = it->find("protocol");
					if (protocol_it != it->end()){
				                protocol = to_string(*protocol_it);
        	        			protocol = protocol.substr(1, protocol.size() - 2);
					}
					auto path_it = it->find("path");
					if (path_it != it->end()){
						path = to_string(*path_it);
						path = path.substr(1, path.size() - 2);
						port = "0";//dummy
					}else{
						auto cert_it = it->find("certificate");
						if (cert_it != it->end()){
							cert = to_string(*cert_it);
							cert = cert.substr(1, cert.size() - 2);
						}
						auto key_it = it->find("key");
						if (key_it != it->end()){
							key = to_string(*key_it);
							key = key.substr(1, key.size() - 2);
						}
						auto trusted_it = it->find("trusted");
						if (trusted_it != it->end()){
							trusted = to_string(*trusted_it);
							trusted = trusted.substr(1, trusted.size() - 2);
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
					fmt::print("Source protocol {} ip {} key {} cert {} trusted {} port {} path {}\n",protocol, ipaddr, key,cert,trusted,port,path);
					_sources.addSource(protocol, ipaddr != "" ? ipaddr : path, key, cert, trusted, std::stoi(port));
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
				auto sources_it = jsonPayload.find("sources");
				auto sinks_it = jsonPayload.find("sinks");
				if (sources_it == jsonPayload.end() || sinks_it == jsonPayload.end()) {
					throw std::invalid_argument("configuration requires 'sources' and 'sinks'");
				}
				if (sources_it->is_object() || sinks_it->is_object()) {
					process_pipeline_sources(*sources_it);
					auto transforms_it = jsonPayload.find("transforms");
					if (transforms_it != jsonPayload.end()) {
						process_pipeline_transforms(*transforms_it);
					}
					process_pipeline_sinks(*sinks_it);
				} else {
					process_sources(jsonPayload);
					process_sinks(jsonPayload);
					if (_sources.size() == 0 || _sinks.size() == 0) {
						throw std::invalid_argument(
							"configuration must contain at least one source and sink");
					}
					for (size_t i = 0; i < _sources.size(); ++i) {
						_pipeline.add({fmt::format("source_{}", i), _sources[i].protocol, {}});
					}
					for (size_t i = 0; i < _sinks.size(); ++i) {
						_sinks[i].id = fmt::format("sink_{}", i);
						_pipeline.add({fmt::format("sink_{}", i), _sinks[i].database_type,
							i == 0 ? std::vector<seastar::sstring>{
								fmt::format("source_{}", std::min(i, _sources.size() - 1))} :
								std::vector<seastar::sstring>{}});
					}
				}
				_pipeline.validate();
			}
			seastar::sstring _json_file;
			Sources _sources;
			Sinks _sinks;
			PipelineGraph _pipeline;
	};
}
