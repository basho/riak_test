#!/usr/bin/env bash
#
# Install a devrel or stagedevrel version of Riak for riak_test
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

# just bail out if things go south
set -e

: ${RT_DEST_DIR:="$HOME/rt/riak"}

cwd=$(pwd)
echo -n " - Determining version: "
if [ -z "${RT_VERSION+xxx}" ] || ([ -z "$RT_VERSION" ] && [ "${RT_VERSION+xxx}" = "xxx" ]); then
    if [ -f $cwd/dependency_manifest.git ]; then
        # For packaged distributions
        RT_VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
    else
        echo "Making $(pwd) a tagged release:"
        #TAGS=`git describe --tags`
        CURRENT=`git rev-parse HEAD`
        HEAD=`git show-ref | grep HEAD | cut -f1 -d" "`
        #if [ -n "`echo ${TAGS} | grep riak_ee`" ]; then
            # For riak_ee
        #    RT_VERSION=`echo ${TAGS} | awk '{sub(/riak_ee-/,"",$0);print}'`
        #else
            # For open source riak
        #    RT_VERSION=`echo ${TAGS} | awk '{sub(/riak-/,"",$0);print}'`
        #fi
        # If we are on the tip, call the version "head"
        if [ "${CURRENT}" == "${HEAD}" ]; then
            RT_VERSION="head"
        else
            RT_VERSION="$(git describe --tags)"
        fi
    fi
fi
echo $RT_VERSION

# Create the RT_DEST_DIR directory if it does not yet exist
if [ ! -d $RT_DEST_DIR ]; then
    mkdir -p $RT_DEST_DIR
fi

# Reinitialize the Git repo if it already exists,
# including removing untracked files
cd $RT_DEST_DIR
if [ -d ".git" ]; then
    echo " - Resetting existing $RT_DEST_DIR"
    git reset HEAD --hard > /dev/null 2>&1
    git clean -fd > /dev/null 2>&1
fi

RT_VERSION_DIR=$RT_DEST_DIR/$RT_VERSION
echo " - Removing and recreating $RT_VERSION_DIR"
rm -rf $RT_VERSION_DIR
mkdir $RT_VERSION_DIR
cd $cwd
echo " - Copying devrel to $RT_VERSION_DIR"
if [ ! -d "dev" ]; then
    echo "You need to run \"make devrel\" or \"make stagedevrel\" first"
    exit 1
fi
cd dev

# Clone the existing dev directory into RT_DEST_DIR
for i in `ls`; do cp -p -P -R $i $RT_DEST_DIR/$RT_VERSION/; done

    VERSION_FILE=$RT_VERSION_DIR/VERSION
    echo " - Writing $VERSION_FILE"
    echo -n $RT_VERSION > $VERSION_FILE

    cd $RT_DEST_DIR
    if [ -d ".git" ]; then
        echo " - Reinitializing git state"
        git add --ignore-removal -f .
        git commit -a -m "riak_test init" --amend > /dev/null 2>&1
    else
        git init

        ## Some versions of git and/or OS require these fields
        git config user.name "Riak Test"
        git config user.email "dev@basho.com"

        git add --ignore-removal .
        git commit -a -m "riak_test init" > /dev/null
        echo " - Successfully completed initial git commit of $RT_DEST_DIR"
    fi
