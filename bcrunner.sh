#!/bin/bash

test_name=$1
bin_size=$2
version=$3
perf_test=$4
backend=$5
cuttle=$5

if [ -z $version -o -z $test_name -o -z $bin_size ]; then
    echo "out"
    exit 1
fi

if [ -z $perf_test ]; then
    perf_test="get_put"
fi

if [ -z $backend ]; then
    backend="riak_kv_bitcask_backend"
fi

if [ ! -z $cuttle ]; then
    cuttle="--cuttle false"
else 
    cuttle=""
fi

./riak_test -c rtperf2 -t $perf_test -b $backend -- --restart true --prepop true \
    --run-time 120 --target-pct 120 --ram-size 48 \
    --bin-size $bin_size --name $test_name --bin-type exponential \
    --version $version $cuttle

./riak_test -c rtperf2 -t $perf_test -b $backend -- --restart true --prepop true \
    --run-time 120 --target-pct 70 --ram-size 48 \
    --bin-size $bin_size --name $test_name --bin-type exponential \
    --version $version $cuttle

./riak_test -c rtperf2 -t $perf_test -b $backend -- --restart true --prepop true \
    --run-time 120 --target-pct 20 --ram-size 48 \
    --bin-size $bin_size --name $test_name --bin-type exponential \
    --version $version $cuttle

