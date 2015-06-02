#!/usr/bin/env bash

# bail out if things go south
set -e

# Creates a mixed-version directory structure for running riak_test
# using rtdev-mixed.config settings. Should be run inside a directory
# that contains devrels for prior Riak releases. Easy way to create this
# is to use the rtdev-build-releases.sh script

: ${RT_DEST_DIR:="$HOME/rt/riak"}

echo "Setting up releases from $(pwd):"
echo " - Creating $RT_DEST_DIR"

rm -rf $RT_DEST_DIR
mkdir -p $RT_DEST_DIR

count=$(ls */dev 2> /dev/null | wc -l)
if [ "$count" -ne "0" ]
then
    for rel in */dev; do
        vsn=$(dirname "$rel")
        echo " - Initializing $RT_DEST_DIR/$vsn"
        mkdir -p "$RT_DEST_DIR/$vsn"
        cp -p -P -R "$rel" "$RT_DEST_DIR/$vsn"
        (cd "$rel"; VERSION=`git describe --tags`; echo $VERSION > $RT_DEST_DIR/$vsn/VERSION)
    done
else
    # This is useful when only testing with 'current'
    # The repo still needs to be initialized for current
    # and we don't want to bomb out if */dev doesn't exist
    touch $RT_DEST_DIR/.current_init
    echo "No devdirs found. Not copying any releases."
fi

cd $RT_DEST_DIR
git init 

## Some versions of git and/or OS require these fields
git config user.name "Riak Test"
git config user.email "dev@basho.com"

git add .
git commit -a -m "riak_test init" > /dev/null
echo " - Successfully completed initial git commit of $RT_DEST_DIR"
