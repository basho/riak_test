#!/bin/bash

echo "================== riak_test Omnibus Installer =================="
echo
echo "This is an omnibus script that builds all the necessary versions "
echo "of Erlang and Riak (including the latest from Github) for running"
echo "riak_test and installs them into /tmp/rt.                        "
echo
echo -n "Are you sure you want to continue? [Y|n] "
read continue
if [[ $continue == n || $continue == N ]]; then
    echo
    echo "Aborting install!"
    exit 1
fi

ORIGDIR=`pwd`
SCRIPT_DIR=`dirname $0`
CURRENT_OTP=${CURRENT_OTP:-$HOME/erlang-R15B01}

echo
echo "= Building Riak Releases ========================================"
echo
echo "Prepping build directory."
mkdir -p /tmp/rt-builds

cd /tmp/rt-builds

echo
source $SCRIPT_DIR/rtdev-build-releases.sh
echo "Cloning Riak master into current."
git clone git://github.com/basho/riak.git current > /dev/null 2>&1
echo
build "current" $CURRENT_OTP

echo "= Installing Riak Releases ======================================"
echo
source $SCRIPT_DIR/rtdev-setup-releases.sh

cd $ORIGDIR
