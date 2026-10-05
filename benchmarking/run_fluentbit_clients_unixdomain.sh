#!/bin/bash
#taskset -c 15-20 /home/vsuraev/fluent-bit/build/bin/fluent-bit -c fluent-bit_unixdomain.conf 
taskset -c 13-20 fluent-bit -c fluent-bit_unixdomain.conf 
