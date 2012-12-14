#!/bin/bash

ORIGDIR=`pwd`
pushd `dirname $0` > /dev/null
SCRIPT_DIR=`pwd`
popd > /dev/null

# OTP Precedence: CURRENT_OTP > HOME/ERLANG_BASE{VERSION} > KERL_DIR
if [ "x$ERLANG_BASE" = "x" ]; then 
    ERLANG_BASE="erlang-"
fi 
OTP_VERSION=R15B01

CURRENT_OTP=${CURRENT_OTP:-$HOME/${ERLANG_BASE}${OTP_VERSION}}
if [ ! -d $CURRENT_OTP ]; then
    CURRENT_OTP=$HOME/.kerl/builds/erlang-${OTP_VERSION}/release_${OTP_VERSION}
fi

if [ ! -d $CURRENT_OTP ]; then
    echo "Could not find the Erlang/OTP directory."
    echo "Specify it through an absolute path in $CURRENT_OTP "
    echo "or prefix $ERLANG_BASE if in your home directory"
    echo "i.e. if in ~/erlang-R15B01, $ERLANG_BASE=erlang-"
    exit 1
fi

echo "OTP Directory :    $CURRENT_OTP"

if [ -n "$DEBUG_RTDEV" ]; then
    echo "= Configuration ================================================="
    echo "Build dir:       $ORIGDIR"
    echo "rtdev-* scripts: $SCRIPT_DIR"
    echo "Erlang:          $CURRENT_OTP"
    echo
fi

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

echo
echo "= Building Riak Releases ========================================"
echo

source $SCRIPT_DIR/rtdev-build-releases.sh

if [[ `uname -s` =~ ^Darwin ]]; then
  if [[ `sw_vers|grep ProductVersion|awk '{print $2}'` > "10.7" ]]; then
    echo
    echo "= Patching OSX > 10.7 ======================================"
    echo
    source $SCRIPT_DIR/rtdev-lion-fix.sh
  fi
fi

echo "= Installing Riak Releases ======================================"
echo
source $SCRIPT_DIR/rtdev-setup-releases.sh

echo
echo "= Building and Installing Riak from Git ========================="
echo

cd $ORIGDIR
build "current" $CURRENT_OTP "" "git://github.com/basho/riak.git"
echo
cd current
source $SCRIPT_DIR/rtdev-current.sh

cd $ORIGDIR
echo
echo "= Build complete! ==============================================="
