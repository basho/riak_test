#!/usr/bin/env bash
set -e

# $1 is group $2 is backend and $3 is config $4 is results directory


echo "Running $1 with $2 and config $CONFIG"

if [ "$1" != "yoko" ]; then
    TEST_EBIN=ebin
else    
    TEST_EBIN=~/yokozuna/riak_test/ebin
fi

if [ "$1" != "2i" ]; then
    BACKEND=${2:-bitcask}
else
    BACKEND=${2:-leveled}
fi

LOG=$1-$(date +"%FT%H%M")-$BACKEND

CONFIG=${3:-rtdev}
BASE_DIR=${4:-$LOG}

echo "Backend is $BACKEND"

# copy test beams
echo "Copying beams"
mkdir -p $BASE_DIR/group_tests/$1
while read t; do  cp $TEST_EBIN/$t.beam $BASE_DIR/group_tests/$1;done <groups/$1

# run tests independently
mkdir -p $BASE_DIR/results/$1
echo "Running tests"

for t in $BASE_DIR/group_tests/$1/*; do ./riak_test --batch -c $CONFIG -b $BACKEND -t $t; done | tee $BASE_DIR/results/$1/log

# output results
echo "making summary"
while read t; do grep $t-$BACKEND $BASE_DIR/results/$1/log ;done <groups/$1 | tee $BASE_DIR/results/$1/summary
