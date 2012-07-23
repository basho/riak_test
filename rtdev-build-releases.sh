#!/bin/bash

# You need to use this script once to build a set of devrels for prior
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

R13B04=/Users/jtuple/erlang-R13B04
R14B03=/Users/jtuple/erlang-R14B03
R14B04=/Users/jtuple/erlang-R14B04
R15B01=/Users/jtuple/erlang-R15B01

build()
{
    SRCDIR=$1
    ERLROOT=$2

    echo
    echo "Building $SRCDIR"
    cd $SRCDIR

    RUN="env PATH=$PATH:$ERLROOT/bin:$ERLROOT/lib/erlang/bin \
             C_INCLUDE_PATH=$ERLROOT/usr/include \
             LD_LIBRARY_PATH=$ERLROOT/usr/lib"
    $RUN make
    $RUN make devrel
    cd ..
}

# Download Riak release source
wget http://downloads.basho.com/riak/riak-0.14/riak-0.14.2.tar.gz
wget http://downloads.basho.com/riak/riak-1.0.3/riak-1.0.3.tar.gz
wget http://downloads.basho.com/riak/riak-1.1.4/riak-1.1.4.tar.gz

tar -xzf riak-0.14.2.tar.gz
build "riak-0.14.2" $R13B04

tar -xzf riak-1.0.3.tar.gz
build "riak-1.0.3" $R14B03

tar -xzf riak-1.1.4.tar.gz
build "riak-1.1.4" $R14B03
