#!/bin/bash
set -e
shopt -s nullglob
if [ $# -lt 2 ]; then
    echo "runtests.sh <config> <test-directory>"
    exit 1
fi
CFG=$1
TESTS=$2
for src in $TESTS/*.erl; do
    if [ -z $src ]; then
        continue
    fi
    base=`basename $src`
    test=${base%.erl}
    echo ./riak_test $CFG $test
    ./riak_test $CFG $test
done
