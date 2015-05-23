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

if [ -z "$1" ]; then
    echo "Fist argument must be the devrel number"
    exit 1
fi

# Default number of nodes to build is 8
NUM_NODES=8
if [ ! -z "$2" ]; then
    NUM_NODES=$2
fi

RT_DEVREL=$1

echo "Building devrel ${RT_DEVREL}"
rm -rf dev
START=`expr \( $RT_DEVREL - 1 \) \* $RT_MAX_NODES + 1`
END=`expr $START + $NUM_NODES - 1`
for j in `seq ${START} ${END}`; do
    $RUN make dev$j
done
