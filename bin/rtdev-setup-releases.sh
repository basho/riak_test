#!/bin/bash

# Creates a mixed-version directory structure for running riak_test
# using rtdev-mixed.config settings. Should be run inside a directory
# that contains devrels for prior Riak releases. Easy way to create this
# is to use the rtdev-build-releases.sh script

echo "Setting up releases from $(pwd):"
echo " - Creating /tmp/rt"
rm -rf /tmp/rt
mkdir /tmp/rt
for rel in */dev; do
    vsn=$(dirname "$rel")
    echo " - Initializing /tmp/rt/$vsn"
    mkdir "/tmp/rt/$vsn"
    cp -a "$rel" "/tmp/rt/$vsn"
done
cd /tmp/rt
echo " - Creating the git repository"
git init > /dev/null 2>&1
git add .
git commit -a -m "riak_test init" > /dev/null 2>&1
echo "Done!"
