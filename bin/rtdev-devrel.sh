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

# Run this script inside a Riak(_EE) build to construct a series
# of devrel nodes in a cluster.  The first argument is the devrel
# number; the second is the number of nodes to build

# Maximum number of nodes on each devrel
: ${RT_MAX_NODES:=10}

# Hostname to be specified for each devrel
: ${RT_HOST:="127.0.0.1"}

# Number of nodes to generate for each devrel
: ${DEVNODES:="8"}

if [ -z "$1" ]; then
    echo "Fist argument must be the devrel number"
    exit 1
fi

RT_DEVREL=$1

echo "Building devrel ${RT_DEVREL}"
rm -rf dev
mkdir -p dev

START=`expr \( $RT_DEVREL - 1 \) \* $RT_MAX_NODES + 1`
END=`expr $START + $DEVNODES - 1`
for devrel in `seq ${START} ${END}`; do
    #
    # Stolen from https://github.com/basho/riak_ee/blob/develop/rel/gen_dev
    #
    NAME=dev$devrel
    TEMPLATE=rel/vars/dev_vars.config.src
    VARFILE=rel/vars/${NAME}_vars.config

    ## Allocate 10 ports per node
    ##   .
    ##   .7 - http
    NODE="$NAME@$RT_HOST"

    BASE=$((10000 + 10 * $devrel))
    SNMPPORT=$(($BASE))
    YZSOLRJMXPORT=$(($BASE + 3))
    YZSOLRPORT=$(($BASE + 4))
    JMXPORT=$(($BASE + 5))
    CM_PORT=$(($BASE + 6))
    PBPORT=$(($BASE + 7))
    WEBPORT=$(($BASE + 8))
    HANDOFFPORT=$(($BASE + 9))

    echo "Generating $NAME - node='$NODE' yzsolrjmx=$YZSOLRJMXPORT yzsolr=$YZSOLRPORT snmp=$SNMPPORT pbc=$PBPORT http=$WEBPORT handoff=$HANDOFFPORT"
    sed -e "s/@NODE@/$NODE/" \
        -e "s/@SNMPPORT@/$SNMPPORT/" \
        -e "s/@YZSOLRJMXPORT@/$YZSOLRJMXPORT/" \
        -e "s/@YZSOLRPORT@/$YZSOLRPORT/" \
        -e "s/@JMXPORT@/$JMXPORT/" \
        -e "s/@PBPORT@/$PBPORT/" \
        -e "s/@WEBPORT@/$WEBPORT/" \
        -e "s/@CM_PORT@/$CM_PORT/" \
        -e "s/@HANDOFFPORT@/$HANDOFFPORT/" < $TEMPLATE > $VARFILE

    echo $PWD
	(cd rel && ../rebar generate target_dir="../dev/${NAME}" overlay_vars="vars/${NAME}_vars.config")
done
