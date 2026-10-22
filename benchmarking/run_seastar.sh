#!/bin/bash
ulimit -n 65535
../bin/rapid_ingestor --config ./config.json --cpus 0-60
