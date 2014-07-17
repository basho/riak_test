#!/bin/sh

cp examples/riak_test.config.perf ~/.riak_test.config
make clean
make

echo "Running the CRDT team benchmark."
./riak_test -c rtperf -t crdt_team_map -v -- --run-time 20 --ram-size 61440 --name crdt_team_map

echo "Running the get/put benchmark."
./riak_test -c rtperf -t get_put -v -- --run-time 20 --ram-size 61440 --name get_put

echo "Running the consistent get/put benchmark."
./riak_test -c rtperf -t consistent_get_put -v -- --run-time 20 --ram-size 61440 --name consistent_get_put
