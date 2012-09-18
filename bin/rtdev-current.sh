#!/bin/bash

echo "Making $(pwd) the current release:"
cwd=$(pwd)
echo -n " - Determining version: "
if [ -f $cwd/dependency_manifest.git ]; then
    VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
else
    VERSION="$(git describe --tags)-$(git branch | awk '/\*/ {print $2}')"
fi
echo $VERSION
cd /tmp/rt
echo " - Resetting existing /tmp/rt"
git reset HEAD --hard > /dev/null 2>&1
git clean -fd > /dev/null 2>&1
echo " - Removing and recreating /tmp/rt/current"
rm -rf /tmp/rt/current
mkdir /tmp/rt/current
cd $cwd
echo " - Copying devrel to /tmp/rt/current"
cp -a dev /tmp/rt/current
echo " - Writing /tmp/rt/current/VERSION"
echo -n $VERSION > /tmp/rt/current/VERSION
cd /tmp/rt
echo " - Reinitializing git state"
git add .
git commit -a -m "riak_test init" --amend > /dev/null 2>&1
echo "Done!"
