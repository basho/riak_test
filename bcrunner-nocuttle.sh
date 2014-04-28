#!/bin/bash

test_name=$1
bin_size=$2
version=$3

if [ -z $version -o -z $test_name -o -z $bin_size ]; then
    echo "out"
    exit 1
fi

./riak_test -c rtperf2 -t get_put -- --restart true --prepop true \
    --run-time 120 --target-pct 120 --ram-size 48 \
    --bin-size $bin_size --name $test_name --bin-type exponential \
    --version $version --cuttle false

./riak_test -c rtperf2 -t get_put -- --restart true --prepop true \
    --run-time 120 --target-pct 70 --ram-size 48 \
    --bin-size $bin_size --name $test_name --bin-type exponential \
    --version $version --cuttle false

./riak_test -c rtperf2 -t get_put -- --restart true --prepop true \
    --run-time 120 --target-pct 20 --ram-size 48 \
    --bin-size $bin_size --name $test_name --bin-type exponential \
    --version $version --cuttle false

