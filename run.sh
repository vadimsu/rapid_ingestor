#!/bin/bash
ulimit -n 65535
./bin/rapid_ingestor --config ./config.json --reactor-backend io_uring
