#!/usr/bin/env bash

# Creates a mixed-version directory structure for running riak_test
# using rtdev-mixed.config settings. Should be run inside a directory
# that contains devrels for prior Riak releases. Easy way to create this
# is to use the rtdev-build-releases.sh script

: ${RT_DEST_DIR:="$HOME/rt/riak"}

echo "Setting up releases from $(pwd):"
echo " - Creating $RT_DEST_DIR"

rm -rf $RT_DEST_DIR
mkdir -p $RT_DEST_DIR
for rel in */dev; do
    vsn=$(dirname "$rel")
    echo " - Initializing $RT_DEST_DIR/$vsn"
    mkdir -p "$RT_DEST_DIR/$vsn"
    cp -p -P -R "$rel" "$RT_DEST_DIR/$vsn"
done
cd $RT_DEST_DIR
echo " - Creating the git repository"
git init > /dev/null 2>&1

## Some versions of git and/or OS require these fields
git config user.name "Riak Test"
git config user.email "dev@basho.com"

git add .
git commit -a -m "riak_test init" > /dev/null 2>&1
