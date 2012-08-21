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

R13B04=${R13B04:-$HOME/erlang-R13B04}
R14B03=${R14B03:-$HOME/erlang-R14B03}
R14B04=${R14B04:-$HOME/erlang-R14B04}
R15B01=${R15B01:-$HOME/erlang-R15B01}

checkbuild()
{
    ERLROOT=$1
    
    if [ ! -d $ERLROOT ]; then
        echo -n "$ERLROOT cannot be found, install kerl? [y|N]: "
        read ans
        if [[ $ans == n || $ans == N ]]; then
            exit 1
        fi
    fi
}

kerl()
{
    RELEASE=$1
    BUILDNAME=$2

    if [ ! -x kerl ]; then
        curl -O https://raw.github.com/spawngrid/kerl/master/kerl; chmod a+x kerl
    fi

    ./kerl build $RELEASE $BUILDNAME
    ./kerl install $BUILDNAME $HOME/$BUILDNAME
}

build()
{
    SRCDIR=$1
    ERLROOT=$2

    if [ ! -d $ERLROOT ]; then
        BUILDNAME=`basename $ERLROOT`
        RELEASE=`echo $BUILDNAME | awk -F- '{ print $2 }'`
        kerl $RELEASE $BUILDNAME
    fi

    echo
    echo "Building $SRCDIR"
    cd $SRCDIR

    RUN="env PATH=$ERLROOT/bin:$ERLROOT/lib/erlang/bin:$PATH \
             C_INCLUDE_PATH=$ERLROOT/usr/include \
             LD_LIBRARY_PATH=$ERLROOT/usr/lib"
    echo $RUN
    $RUN make && $RUN make devrel
    cd ..
}

checkbuild $R13B04
checkbuild $R14B03
checkbuild $R14B04
checkbuild $R15B01

if [ $1 = "-ee" ]; then
    # Download Riak EE release source, need s3cmd configured
    s3cmd get --continue s3://builds.basho.com/riak_ee/riak_ee-0.14/0.14.2/riak_ee-0.14.2.tar.gz
    s3cmd get --continue s3://builds.basho.com/riak_ee/1.0/1.0.3/riak_ee-1.0.3.tar.gz
    s3cmd get --continue s3://builds.basho.com/riak_ee/1.1/1.1.4/riak_ee-1.1.4.tar.gz
    s3cmd get --continue s3://builds.basho.com/riak_ee/1.2/1.2.0/riak_ee-1.2.0.tar.gz

    tar -xzf riak_ee-0.14.2.tar.gz
    build "riak_ee-0.14.2" $R13B04

    tar -xzf riak_ee-1.0.3.tar.gz
    build "riak_ee-1.0.3" $R14B03

    tar -xzf riak_ee-1.1.4.tar.gz
    build "riak_ee-1.1.4" $R14B04

    tar -xzf riak_ee-1.2.0.tar.gz
    build "riak_ee-1.2.0" $R15B01
else
    # Download Riak release source
    wget -c http://downloads.basho.com/riak/riak-0.14/riak-0.14.2.tar.gz
    wget -c http://downloads.basho.com/riak/riak-1.0.3/riak-1.0.3.tar.gz
    wget -c http://downloads.basho.com/riak/riak-1.1.4/riak-1.1.4.tar.gz
    wget -c http://s3.amazonaws.com/downloads.basho.com/riak/1.2/1.2.0/riak-1.2.0.tar.gz
    
    tar -xzf riak-0.14.2.tar.gz
    build "riak-0.14.2" $R13B04

    tar -xzf riak-1.0.3.tar.gz
    build "riak-1.0.3" $R14B03

    tar -xzf riak-1.1.4.tar.gz
    build "riak-1.1.4" $R14B04

    tar -xvf riak-1.2.0.tar.gz
    build "riak-1.2.0" $R15B01
fi
