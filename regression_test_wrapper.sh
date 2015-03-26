#!/bin/bash

# Bail out on error ...
#set -e

if [ -z $1 ]; then
  echo "An r_t configuration is required as the first parameter"
  exit 1
fi

ALL_BACKEND_TEST_CASES="always_pass_test,verify_riak_stats,verify_down,verify_staged_clustering,verify_leave,partition_repair,verify_build_cluster,riak_control_authentication,always_fail_test,basic_command_line,jmx_verify,verify_aae,verify_claimant,verify_object_limits,ensemble_interleave,ensemble_byzantine,gh_riak_core_155,pb_security,verify_search,verify_handoff,verify_capabilities,replication/repl_fs_stat_caching,replication/replication2,verify_handoff_mixed,verify_bitcask_tombstone2_upgrade"

BITCASK_BACKEND_TEST_CASES="$ALL_BACKEND_TEST_CASES,loaded_upgrade"
ELEVELDB_BACKEND_TEST_CASES="verify_2i_aae,loaded_upgrade"
MEMORY_BACKEND_TEST_CASES="verify_2i_aae"

ROOT_RESULTS_DIR="results/regression"
RESULTS=`date +"%y%m%d%H%M%s"`
RESULTS_DIR="$ROOT_RESULTS_DIR/$RESULTS"
mkdir -p $RESULTS_DIR

RESULTS_SYMLINK=$ROOT_RESULTS_DIR/current
rm -f $RESULTS_SYMLINK
ln -s $RESULTS $RESULTS_SYMLINK

RT_OPTS="-v --continue -c $1"

echo "Running bitcask regression tests using the following test cases: $BITCASK_BACKEND_TEST_CASES"
./riak_test $RT_OPTS -t $BITCASK_BACKEND_TEST_CASES &> $RESULTS_DIR/bitcask_results.log

echo "Running leveldb regression tests using the following test cases: $ELEVELDB_BACKEND_TEST_CASES"
./riak_test $RT_OPTS -t $ELEVELDB_BACKEND_TEST_CASES -b eleveldb &> $RESULTS_DIR/leveldb_results.log

echo "Running memory regression tests using the following test cases: $MEMORY_BACKEND_TEST_CASES"
./riak_test $RT_OPTS -t $MEMORY_BACKEND_TEST_CASES -b memory &> $RESULTS_DIR/memory_results.log

echo "Results of the test run written to $RESULTS_DIR"
