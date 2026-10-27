#pragma once

// Loads a --template JSON file describing the named/typed fields
// forward_traffic_gen should generate, so synthetic traffic can match a
// specific ClickHouse sink schema variant (see
// RapidIngestor::ClickHouseSink / Config::Sink::columns in
// src/rapid_ingestor_clickhouse_sink.hh and src/rapid_ingestor_config.hh).
//
// Schema (top level):
//   {
//     "tag": "bench.tag",     // optional, used unless --tag is passed explicitly
//     "fields": [ <field>, ... ]
//   }
//
// Each <field> is:
//   { "name":"level", "type":"string", "values":["debug","info","warn","error"] }
//   { "name":"duration_ms", "type":"int64", "min":1, "max":5000 }
//   { "name":"success", "type":"bool" }                      // alternates true/false
//   { "name":"ok", "type":"bool", "value":true }              // fixed
//   { "name":"ctx", "type":"map", "fields":[ <field>, ... ] } // nested record
//
// "type" defaults to "string". String fields without "values" fall back to a
// fixed-size dummy payload ("size", default 16, clamped to 31 - fixstr-only,
// see forward_msgpack_writer.hh). Unknown top-level keys (e.g. "_comment")
// are ignored, matching the rest of this project's JSON configs.
//
// See forward_traffic_gen/templates/*.json for ready-made templates matching
// benchmarking/config_single_column_text.json, config_single_column_json.json,
// config_multi_column.json and config_multi_column_aliasing.json.

#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "nlohmann/json.hpp"
#include "forward_msgpack_writer.hh"

namespace ForwardTrafficGen {

struct Template {
    std::string tag;
    std::vector<FieldSpec> fields;
};

inline FieldSpec parse_field(const nlohmann::json& j) {
    FieldSpec spec;
    spec.name = j.at("name").get<std::string>();

    std::string type = j.value("type", std::string("string"));
    if (type == "int64" || type == "Int64") spec.type = FieldSpec::Type::Int64;
    else if (type == "bool" || type == "Bool") spec.type = FieldSpec::Type::Bool;
    else if (type == "map" || type == "Map") spec.type = FieldSpec::Type::Map;
    else spec.type = FieldSpec::Type::String;

    switch (spec.type) {
        case FieldSpec::Type::String:
            if (j.contains("values")) {
                for (auto& v : j.at("values")) spec.values.push_back(v.get<std::string>());
            }
            spec.size = j.value("size", spec.size);
            break;
        case FieldSpec::Type::Int64:
            spec.min = j.value("min", spec.min);
            spec.max = j.value("max", spec.max);
            break;
        case FieldSpec::Type::Bool:
            if (j.contains("value")) spec.fixed_bool = j.at("value").get<bool>();
            break;
        case FieldSpec::Type::Map:
            for (auto& sub : j.at("fields")) spec.fields.push_back(parse_field(sub));
            break;
    }
    return spec;
}

inline Template load_template(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot open template file: " + path);
    nlohmann::json j;
    in >> j;

    Template tmpl;
    if (j.contains("tag")) tmpl.tag = j.at("tag").get<std::string>();
    for (auto& f : j.at("fields")) tmpl.fields.push_back(parse_field(f));
    return tmpl;
}

} // namespace ForwardTrafficGen
