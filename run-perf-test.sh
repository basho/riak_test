#!/bin/sh

cp examples/riak_test.config.perf ~/.riak_test.config && make clean && make && ./riak_test -c rtperf -t get_put -v
