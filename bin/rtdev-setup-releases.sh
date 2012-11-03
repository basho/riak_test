#!/usr/bin/env bash

# Creates a mixed-version directory structure for running riak_test
# using rtdev-mixed.config settings. Should be run inside a directory
# that contains devrels for prior Riak releases. Easy way to create this
# is to use the rtdev-build-releases.sh script

RT_DEST_DIR=${RT_DEST_DIR?"/tmp/rt"}

echo "Setting up releases from $(pwd):"
echo " - Creating $RT_DEST_DIR"

rm -rf $RT_DEST_DIR
mkdir $RT_DEST_DIR
for rel in */dev; do
    vsn=$(dirname "$rel")
    echo " - Initializing $RT_DEST_DIR/$vsn"
    mkdir "$RT_DEST_DIR/$vsn"
    cp -a "$rel" "$RT_DEST_DIR/$vsn"
done
cd $RT_DEST_DIR
echo " - Creating the git repository"
git init > /dev/null 2>&1
git add .
git commit -a -m "riak_test init" > /dev/null 2>&1
