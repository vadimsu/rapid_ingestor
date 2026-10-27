#!/bin/bash
ulimit -n 65535
#CPU 230-240 bytes 500K/sec client 95%
#./bin/rapid_ingestor --config ./config.json --reactor-backend epoll --overprovisioned -c 12
#CPU 230-240 bytes 500K/sec client 95%
#./bin/rapid_ingestor --config ./config.json --reactor-backend io_uring --overprovisioned -c 12
##CPU 400 bytes 500k/sec
#./bin/rapid_ingestor --config ./config.json --reactor-backend io_uring -c 12
##CPU >1000, bytes 500k/sec
#./bin/rapid_ingestor --config ./config.json --reactor-backend io_uring --overprovisioned -c 12 --poll-mode
#CPU ~200, bytes 500k/sec
#./bin/rapid_ingestor --config ./config.json --reactor-backend io_uring --overprovisioned --idle-poll-time-us 10 -m 2G --reserve-memory 1G --cpus 0-12
##CPU ~200, bytes 500k/sec
#./bin/rapid_ingestor --config ./config.json --reactor-backend epoll --overprovisioned -c 12 --idle-poll-time-us 10
#CPU 600 bytes 500k/sec
#./bin/rapid_ingestor --config ./config.json --reactor-backend epoll --overprovisioned -c 12 --idle-poll-time-us 500
#CPU 600 bytes 520k/sec
#./bin/rapid_ingestor --config ./config.json --reactor-backend io_uring --overprovisioned -c 12 --idle-poll-time-us 500
#./bin/rapid_ingestor --config ./config.json --reactor-backend io_uring --overprovisioned -c 12 --idle-poll-time-us 10
#./bin/rapid_ingestor --config ./config.json --reactor-backend linux-aio --overprovisioned -c 12 --idle-poll-time-us 10
#
#../bin/rapid_ingestor --config ./config.json --reactor-backend epoll --overprovisioned -m 2G --reserve-memory 1G --cpus 0-3
clickhouse-client --password default -q "CREATE DATABASE events"
clickhouse-client --password default -q "DROP TABLE events.multicolumn_events"
#clickhouse-client --password default -q "CREATE TABLE events.multicolumn_events (level Enum8('debug'=1,'info'=2,'warn'=3,'error'=4), duration_ms Int(16), success boolean, ctx Map(Enum8('alice'=1, 'bob'=2, 'carol'= 3), String(15)))"
clickhouse-client --password default -q "CREATE TABLE events.multicolumn_events (level Enum8('debug'=1,'info'=2,'warn'=3,'error'=4), duration_ms Int(16), success boolean, ctx JSON"
../bin/rapid_ingestor --config ./config_multi_column.json --overprovisioned --cpus 0-7
