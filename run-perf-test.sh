#!/bin/sh

cp riak_test.config.example ~/.riak_test.config && make clean && make && ./riak_test -c rtperf -t get_put -v
