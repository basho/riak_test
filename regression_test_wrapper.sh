#!/bin/bash

# Bail out on error ...
set -e

if [ -z $1 ]; then
  echo "An r_t configuration is required as the first parameter"
  exit 1
fi

TEST_CASES="always_pass_test,verify_riak_stats,verify_down,verify_staged_clustering,verify_membackend,verify_leave,partition_repair,verify_build_cluster,riak_control_authentication,always_fail_test,basic_command_line,jmx_verify,verify_aae,verify_claimant,verify_object_limits,ensemble_interleave,ensemble_byzantine"

./riak_test -v -c $1 -t $TEST_CASES
