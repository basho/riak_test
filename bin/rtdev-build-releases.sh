#!/usr/bin/env bash

# You need to use this script once to build a set of stagedevrels for prior
# releases of Riak (for mixed version / upgrade testing). You should
# create a directory and then run this script from within that directory.
# I have ~/test-releases that I created once, and then re-use for testing.
#
# See rtdev-setup-releases.sh as an example of setting up mixed version layout
# for testing.

# Different versions of Riak were released using different Erlang versions,
# make sure to build with the appropriate version.

# This is based on my usage of having multiple Erlang versions in different
# directories. If using kerl or whatever, modify to use kerl's activate logic.
# Or, alternatively, just substitute the paths to the kerl install paths as
# that should work too.

: ${R15B01:=$HOME/erlang-R15B01}
: ${R16B02:=$HOME/erlang-R16B02}

# By default the Open Source version of Riak will be used, but for internal
# testing you can override this variable to use `riak_ee` instead
: ${RT_USE_EE:=""}
GITURL_RIAK="git://github.com/basho/riak"
GITURL_RIAK_EE="git@github.com:basho/riak_ee"


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
                curl -O https://raw.github.com/spawngrid/kerl/master/kerl > /dev/null 2>&1; chmod a+x kerl
            fi
        fi
    fi
}

kerl()
{
    RELEASE=$1
    BUILDNAME=$2

    echo " - Building Erlang $RELEASE (this could take a while)"
    ./kerl build $RELEASE $BUILDNAME  > /dev/null 2>&1
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] Kerl build $BUILDNAME failed"
        exit 1
    fi

    echo " - Installing $RELEASE into $HOME/$BUILDNAME"
    ./kerl install $BUILDNAME $HOME/$BUILDNAME  > /dev/null 2>&1
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] Kerl install $BUILDNAME failed"
        exit 1
    fi
}

build()
{
    SRCDIR=$1
    ERLROOT=$2
    TAG="$3"
    if [ -z "$RT_USE_EE" ]; then
        GITURL=$GITURL_RIAK
        GITTAG=riak-$TAG
    else
        GITURL=$GITURL_RIAK_EE
        GITTAG=riak_ee-$TAG
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
    if [ $GITRES -eq 0 -a -n "$TAG" ]; then
        cd $SRCDIR
        git checkout $GITTAG
        GITRES=$?
        cd ..
    fi

    RUN="env PATH=$ERLROOT/bin:$ERLROOT/lib/erlang/bin:$PATH \
             C_INCLUDE_PATH=$ERLROOT/usr/include \
             LD_LIBRARY_PATH=$ERLROOT/usr/lib"
    fix_riak_1_3 $SRCDIR $TAG "$RUN"

    echo " - Building stagedevrel in $SRCDIR (this could take a while)"
    cd $SRCDIR

    $RUN make all stagedevrel
    RES=$?
    if [ "$RES" -ne 0 ]; then
        echo "[ERROR] make stagedevrel failed"
        exit 1
    fi
    cd ..
    echo " - $SRCDIR built."
}

# Riak 1.3 has a few artifacts which need to be updated in order to build
# properly
fix_riak_1_3()
{
	SRCDIR=$1
	TAG="$2"
	RUN="$3"

    if [ "`echo $TAG | cut -d . -f1-2`" != "1.3" ]; then
        return 0
    fi

    echo "- Patching Riak 1.3.x"
    cd $SRCDIR
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

build "riak-1.4.10" $R15B01 1.4.10
echo
if [ -z "$RT_USE_EE" ]; then
	build "riak-1.3.2" $R15B01 1.3.2
else
	build "riak-1.3.4" $R15B01 1.3.4
fi
echo
