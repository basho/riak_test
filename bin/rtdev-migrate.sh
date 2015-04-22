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

# just bail out if things go south
set -e

: ${RT_DEST_DIR:="$HOME/rt/riak"}
cd ${RT_DEST_DIR}

# New "standard" for current version of Riak
if [ -d current ]; then
    echo "Renaming current to head"
    mv -f current riak-head
fi

# Chop off the "riak-" prefix
for name in riak-*; do
    if [ -d "${name}" ]; then
	    NEW_NAME=`echo ${name} | cut -d- -f2`
	    echo "Renaming ${name} to ${NEW_NAME}"
	    mv ${name} ${NEW_NAME}
    fi
done

# Remove the intermediate "dev" directory
for version in `ls -1`; do
    if [ -d "${version}/dev" ]; then
        echo "Removing the dev directory from ${version}"
        cd ${version}/dev
        mv dev* ..
        cd ..
        rmdir dev
        cd ..
    fi
done

# Set up local Git repo
if [ -d ".git" ]; then
    echo " - Reinitializing git state"
    git add -f --ignore-removal .
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
