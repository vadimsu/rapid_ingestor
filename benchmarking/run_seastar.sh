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
../bin/rapid_ingestor --config ./config.json --reactor-backend epoll --overprovisioned -m 2G --reserve-memory 1G --cpus 0-3
