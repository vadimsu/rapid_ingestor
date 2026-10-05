// Seastar-based Fluent Forward protocol traffic generator.
//
// Opens N concurrent TCP (or Unix domain) connections per core and streams
// MessagePack Forward-mode messages ([tag, [[time, record], ...]]) toward a
// target endpoint as fast as possible (or at a bounded rate), for load
// testing rapid_ingestor or any other Fluent Forward protocol consumer.
//
// Example:
//   ./bin/forward_traffic_gen --host 127.0.0.1 --port 24224 -c 4 \
//       --connections 8 --entries-per-batch 20 --fields 6 --field-size 24
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>
#include <chrono>
#include <vector>
#include "forward_msgpack_writer.hh"

namespace bpo = boost::program_options;

namespace {

struct Stats {
    uint64_t messages = 0;
    uint64_t batches = 0;
    uint64_t bytes = 0;
    Stats& operator+=(const Stats& o) {
        messages += o.messages;
        batches += o.batches;
        bytes += o.bytes;
        return *this;
    }
};

seastar::socket_address make_target_address(const seastar::sstring& host, uint16_t port,
                                              const seastar::sstring& unix_path) {
    if (!unix_path.empty()) {
        return seastar::socket_address(seastar::unix_domain_addr{unix_path});
    }
    seastar::net::inet_address addr(host);
    return seastar::make_ipv4_address({addr, port});
}

class Worker {
public:
    Stats getStats() const { return _stats; }

    seastar::future<> stop() {
        _stop_requested = true;
        return seastar::make_ready_future<>();
    }

    seastar::future<> run(seastar::socket_address target, seastar::sstring tag,
                           unsigned connections, unsigned entries_per_batch, unsigned fields,
                           unsigned field_size, uint64_t batches_limit, uint32_t rate_per_sec) {
        std::vector<seastar::future<>> conns;
        conns.reserve(connections);
        for (unsigned i = 0; i < connections; ++i) {
            conns.push_back(run_connection(target, tag, entries_per_batch, fields, field_size,
                                            batches_limit, rate_per_sec));
        }
        co_await seastar::when_all_succeed(std::move(conns));
    }

private:
    seastar::future<> run_connection(seastar::socket_address target, seastar::sstring tag,
                                      unsigned entries_per_batch, unsigned fields, unsigned field_size,
                                      uint64_t batches_limit, uint32_t rate_per_sec) {
        auto tmpl = ForwardTrafficGen::build_forward_template(tag, entries_per_batch, fields, field_size);
        try {
            auto fd = co_await seastar::connect(target);
            auto out = fd.output();
            uint64_t sent = 0;
            auto next_tick = std::chrono::steady_clock::now();
            auto interval = rate_per_sec > 0
                ? std::chrono::microseconds(1000000 / rate_per_sec)
                : std::chrono::microseconds(0);
            while (!_stop_requested && (batches_limit == 0 || sent < batches_limit)) {
                tmpl.refresh_time();
                co_await out.write(tmpl.bytes.data(), tmpl.bytes.size());
                co_await out.flush();
                _stats.messages += tmpl.entries;
                _stats.batches += 1;
                _stats.bytes += tmpl.bytes.size();
                sent++;
                if (rate_per_sec > 0) {
                    next_tick += interval;
                    auto now = std::chrono::steady_clock::now();
                    if (next_tick > now) co_await seastar::sleep(next_tick - now);
                }
            }
            co_await out.close();
        } catch (const std::exception& e) {
            fmt::print("[shard {}] connection error: {}\n", seastar::this_shard_id(), e.what());
        }
    }

    Stats _stats;
    bool _stop_requested = false;
};

} // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    app.add_options()
        ("host", bpo::value<seastar::sstring>()->default_value("127.0.0.1"), "target host (TCP)")
        ("port", bpo::value<uint16_t>()->default_value(24224), "target port (TCP)")
        ("unix-path", bpo::value<seastar::sstring>()->default_value({}),
            "target unix domain socket path (overrides host/port)")
        ("tag", bpo::value<seastar::sstring>()->default_value("bench.tag"), "Forward protocol tag")
        ("connections", bpo::value<unsigned>()->default_value(1), "concurrent connections per core")
        ("entries-per-batch", bpo::value<unsigned>()->default_value(10), "log entries per Forward message")
        ("fields", bpo::value<unsigned>()->default_value(5), "string fields per record")
        ("field-size", bpo::value<unsigned>()->default_value(16), "bytes per field value (clamped to 31)")
        ("batches", bpo::value<uint64_t>()->default_value(0), "batches per connection, 0 = unlimited")
        ("duration", bpo::value<uint32_t>()->default_value(0), "seconds to run, 0 = unlimited")
        ("rate", bpo::value<uint32_t>()->default_value(0), "max batches/sec per connection, 0 = unlimited");

    return app.run(argc, argv, [&app]() -> seastar::future<> {
        auto& cfg = app.configuration();
        auto host = cfg["host"].as<seastar::sstring>();
        auto port = cfg["port"].as<uint16_t>();
        auto unix_path = cfg["unix-path"].as<seastar::sstring>();
        auto tag = cfg["tag"].as<seastar::sstring>();
        auto connections = cfg["connections"].as<unsigned>();
        auto entries_per_batch = cfg["entries-per-batch"].as<unsigned>();
        auto fields = cfg["fields"].as<unsigned>();
        auto field_size = std::min(cfg["field-size"].as<unsigned>(), 31u);
        auto batches = cfg["batches"].as<uint64_t>();
        auto duration = cfg["duration"].as<uint32_t>();
        auto rate = cfg["rate"].as<uint32_t>();

        auto target = make_target_address(host, port, unix_path);

        auto workers = seastar::make_lw_shared<seastar::sharded<Worker>>();
        co_await workers->start();

        auto last = seastar::make_lw_shared<Stats>();
        seastar::timer<> stats_timer;
        stats_timer.set_callback([workers, last] {
            (void)workers->map_reduce(seastar::adder<Stats>(), &Worker::getStats).then([last](Stats s) {
                fmt::print("total: messages={} batches={} bytes={}  (delta: msgs/s={} bytes/s={})\n",
                    s.messages, s.batches, s.bytes, s.messages - last->messages, s.bytes - last->bytes);
                *last = s;
            });
        });
        stats_timer.arm_periodic(std::chrono::seconds(1));

        auto run_fut = workers->invoke_on_all(&Worker::run, target, tag, connections,
                                               entries_per_batch, fields, field_size, batches, rate);

        if (duration > 0) {
            co_await seastar::sleep(std::chrono::seconds(duration));
            co_await workers->invoke_on_all(&Worker::stop);
        }
        co_await std::move(run_fut);
        stats_timer.cancel();
        co_await workers->stop();
    });
}
