#!/usr/bin/env bash

# just bail out if things go south
set -e

: ${RT_DEST_DIR:="$HOME/rt/riak"}

cwd=$(pwd)
echo -n " - Determining version: "
if [ -z "${VERSION+xxx}" ] || ([ -z "$VERSION" ] && [ "${VERSION+xxx}" = "xxx" ]); then
       if [ -f $cwd/dependency_manifest.git ]; then
           VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
       else
           echo "Making $(pwd) a tagged release:"
           VERSION=`git describe --tags | awk '{sub(/riak-/,"",$0);print}'`
       fi
fi
echo $VERSION
if [ ! -d $RT_DEST_DIR ]; then
    mkdir $RT_DEST_DIR
fi
cd $RT_DEST_DIR
if [ -d ".git" ]; then
    echo " - Resetting existing $RT_DEST_DIR"
    git reset HEAD --hard > /dev/null 2>&1
    git clean -fd > /dev/null 2>&1
fi
echo " - Removing and recreating $RT_DEST_DIR/$VERSION"
rm -rf $RT_DEST_DIR/$VERSION
mkdir $RT_DEST_DIR/$VERSION
cd $cwd
echo " - Copying devrel to $RT_DEST_DIR/$VERSION"
if [ ! -d "dev" ]; then
    echo "You need to run \"make devrel\" or \"make stagedevrel\" first"
    exit 1
fi
cd dev
for i in `ls`; do cp -p -P -R $i $RT_DEST_DIR/$VERSION/; done
echo " - Writing $RT_DEST_DIR/$VERSION/VERSION"
echo -n $VERSION > $RT_DEST_DIR/$VERSION/VERSION
cd $RT_DEST_DIR
if [ -d ".git" ]; then
    echo " - Reinitializing git state"
    git add -f .
    git commit -a -m "riak_test init" --amend > /dev/null 2>&1
else
    git init

    ## Some versions of git and/or OS require these fields
    git config user.name "Riak Test"
    git config user.email "dev@basho.com"

    git add .
    git commit -a -m "riak_test init" > /dev/null
    echo " - Successfully completed initial git commit of $RT_DEST_DIR"
fi
