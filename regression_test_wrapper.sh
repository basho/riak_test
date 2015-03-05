#!/bin/bash

# Bail out on error ...
set -e

if [ -z $1 ]; then
  echo "An r_t configuration is required as the first parameter"
  exit 1
fi

TEST_CASES="always_pass_test,verify_riak_stats,verify_down,verify_staged_clustering,verify_membackend,verify_leave"

./riak_test -c $1 -t $TEST_CASES
