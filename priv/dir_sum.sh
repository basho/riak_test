#!/bin/bash

if [ ! -z $1 ]; then
    RIAK_DIR=$1 
else
    echo "No build supplied" 
    exit 1 
fi 

TMP=/tmp/dir_sum.tmp
SORTED=/tmp/dir_sum.tmp.sorted
rm -f $TMP
rm -f $SORTED

sum_subdir () { 
    SUBDIR=$1
    if [ ! -d $RIAK_DIR/$SUBDIR ]; then 
	exit 2 
    fi
    find $RIAK_DIR/$SUBDIR -type f -print | xargs md5sum -b >> $TMP
}

if [ ! -d $RIAK_DIR ]; then
    exit 2
fi

sum_subdir lib
sum_subdir bin
sum_subdir erts*

sort $TMP > $SORTED

md5sum $SORTED | awk '{print $1}'


rm -f $TMP
rm -f $SORTED

