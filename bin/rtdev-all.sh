#!/usr/bin/env bash
#
# Bootstrap an entire riak_test tree
#
# Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# You need to use this script once to build a set of stagedevrels for prior
# releases of Riak (for mixed version / upgrade testing). You should
# create a directory and then run this script from within that directory.
# I have ~/test-releases that I created once, and then re-use for testing.
#

# Different versions of Riak were released using different Erlang versions,
# make sure to build with the appropriate version.

# This is based on my usage of having multiple Erlang versions in different
# directories. If using kerl or whatever, modify to use kerl's activate logic.
# Or, alternatively, just substitute the paths to the kerl install paths as
# that should work too.

# Set these values for non-default behavior
# Path to the Erlang R15B01 Installation
: ${R15B01:=$HOME/erlang/R15B01}
# Path to the Erlang R15B01 Installation
: ${R16B02:=$HOME/erlang/R16B02}
# Current version of Erlang (for "head" version)
: ${CURRENT_OTP:=$R16B02}
# Label of the "current" version
: ${DEFAULT_VERSION:="riak-head"}
# By default the Open Source version of Riak will be used, but for internal
# testing you can override this variable to use `riak_ee` instead
: ${RT_USE_EE:=""}

ORIGDIR=`pwd`
pushd `dirname $0` > /dev/null
SCRIPT_DIR=`pwd`
popd > /dev/null
GITURL_RIAK="git://github.com/basho/riak"
GITURL_RIAK_EE="git@github.com:basho/riak_ee"

# Determine if Erlang has already been built
checkbuild()
{
    ERLROOT=$1

    if [ ! -d $ERLROOT ]; then
        echo -n " - $ERLROOT cannot be found, install with kerl? [Y|n]: "
        read ans
        if [[ $ans == n || $ans == N ]]; then
            echo
            echo " [ERROR] Can't build $ERLROOT without kerl, aborting!"
            exit 1
        else
            if [ ! -x kerl ]; then
                echo "   - Fetching kerl."
                if [ ! `which curl` ]; then
                    echo "You need 'curl' to be able to run this script, exiting"
                    exit 1
                fi
                curl -O https://raw.githubusercontent.com/spawngrid/kerl/master/kerl; chmod a+x kerl
            fi
        fi
    fi
}

# Build and install Erlangs
kerl()
{
    RELEASE=$1
    BUILDNAME=$2

    export CFLAGS="-g -O2"
    export LDFLAGS="-g"
    if [ -n "`uname -r | grep el6`" ]; then
        export CFLAGS="-g -DOPENSSL_NO_EC=1"
    fi
    if [ "`uname`" == "Darwin" ]; then
        export CFLAGS="-g -O0"
        export KERL_CONFIGURE_OPTIONS="--disable-hipe --enable-smp-support --enable-threads --enable-kernel-poll --without-odbc --enable-darwin-64bit"
    else
        export KERL_CONFIGURE_OPTIONS="--disable-hipe --enable-smp-support --enable-threads --without-odbc --enable-m64-build"
    fi
    echo " - Building Erlang $RELEASE (this could take a while)"
    # Use the patched version of Erlang for R16B02 builds
    if [ "$RELEASE" == "R15B01" ]; then
        ./kerl build git git://github.com/basho/otp.git basho_OTP_R15B01p $BUILDNAME
    elif [ "$RELEASE" == "R16B02" ]; then
        ./kerl build git git://github.com/basho/otp.git r16 $BUILDNAME
    else
        ./kerl build $RELEASE $BUILDNAME
    fi
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] Kerl build $BUILDNAME failed"
        exit 1
    fi

    echo " - Installing $RELEASE into $HOME/$BUILDNAME"
	./kerl install $BUILDNAME $HOME/$BUILDNAME
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] Kerl install $BUILDNAME failed"
        exit 1
    fi
}

# Build stagedevrels for testing
build()
{
    SRCDIR=$1
    ERLROOT=$2
    if [ -z "$RT_USE_EE" ]; then
        GITURL=$GITURL_RIAK
    else
        GITURL=$GITURL_RIAK_EE
    fi

    echo "Building $SRCDIR:"

    checkbuild $ERLROOT
    if [ ! -d $ERLROOT ]; then
        BUILDNAME=`basename $ERLROOT`
        RELEASE=`echo $BUILDNAME | awk -F- '{ print $2 }'`
        kerl $RELEASE $BUILDNAME
    fi

    GITRES=1
    echo " - Cloning $GITURL"
    rm -rf $SRCDIR
    git clone $GITURL $SRCDIR
    GITRES=$?
    if [ $GITRES -eq 0 -a -n "$SRCDIR" ]; then
        cd $SRCDIR
        git checkout $SRCDIR
        GITRES=$?
        cd ..
    fi

    RUN="env PATH=$ERLROOT/bin:$ERLROOT/lib/erlang/bin:$PATH \
             C_INCLUDE_PATH=$ERLROOT/usr/include \
             LD_LIBRARY_PATH=$ERLROOT/usr/lib"
    fix_riak_1_3 $SRCDIR "$RUN"

    echo " - Building stagedevrel in $SRCDIR (this could take a while)"
    cd $SRCDIR

    # For non-tagged builds (i.e. head), use make deps.  Otherwise, use
    # make locked-deps for tagged builds ...
    if [ -n "`echo ${SRCDIR} | grep head`" ]; then
        make deps
    else
        $RUN make locked-deps
    fi

    $RUN make all stagedevrel
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] make stagedevrel failed"
        exit 1
    fi
    echo " - $SRCDIR built."
    $SCRIPT_DIR/rtdev-install.sh
    cd ..
}

# Riak 1.3 has a few artifacts which need to be updated in order to build
# properly
fix_riak_1_3()
{
	SRCDIR=$1
	RUN="$2"

    if [ "`echo $SRCDIR | cut -d- -f2 | cut -d. -f1-2`" != "1.3" ]; then
        return 0
    fi

    echo "- Patching Riak 1.3.x"
    cd $SRCDIR
    if [ "$SRCDIR" == "riak-1.3.2" ]; then
		cat <<EOF | patch
--- rebar.config
+++ rebar.config
@@ -12,6 +12,7 @@
 {deps, [
        {lager_syslog, "1.2.2", {git, "git://github.com/basho/lager_syslog", {tag, "1.2.2"}}},
        {cluster_info, "1.2.3", {git, "git://github.com/basho/cluster_info", {tag, "1.2.3"}}},
+       {meck, "0.7.2", {git, "git://github.com/eproxus/meck", {tag, "0.7.2"}}},
        {riak_kv, "1.3.2", {git, "git://github.com/basho/riak_kv", {tag, "1.3.2"}}},
        {riak_search, "1.3.0", {git, "git://github.com/basho/riak_search",
                                  {tag, "1.3.2"}}},
EOF
	fi
	$RUN make locked-deps
	$RUN make deps
    cd deps/eleveldb/c_src/leveldb/include/leveldb
    cat <<EOF | patch
--- env.h
+++ env.h
@@ -17,6 +17,7 @@
 #include <string>
 #include <vector>
 #include <stdint.h>
+#include <pthread.h>
 #include "leveldb/perf_count.h"
 #include "leveldb/status.h"
EOF
    cd ../../../../../../..
}

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
echo "riak_test and installs them into $HOME/rt/riak.                        "
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

echo
if [ -z "$RT_USE_EE" ]; then
    build "riak-1.3.2" $R15B01
    build "riak-1.4.12" $R15B01
else
    build "riak_ee-1.3.4" $R15B01
    build "riak_ee-1.4.12" $R15B01
    if [ "${DEFAULT_VERSION}" == "riak-head" ]; then
        DEFAULT_VERSION="riak_ee-head"
    fi
    echo "Default version: $DEFAULT_VERSION"
fi
build $DEFAULT_VERSION $R16B02

echo
echo "= Build complete! ==============================================="
