#!/usr/bin/env bash

# bail out if things go south
set -e

: ${RT_DEST_DIR:="$HOME/rt/riak"}
if [[ $# < 1 ]]; then
    echo "Missing release directory as argument, e.g. riak_ee-2.1.2"
    exit 1
fi
RELEASE=$1

echo "Making $(pwd) the $RELEASE release:"
cwd=$(pwd)
echo -n " - Determining version: "
if [ -f $cwd/dependency_manifest.git ]; then
    VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
else
    VERSION="$(git describe --tags)-$(git branch | awk '/\*/ {print $2}')"
fi
if [[ "$RELEASE" == "current" && ! -z "$RT_CURRENT_TAG" ]]; then
    VERSION=$RT_CURRENT_TAG
fi
echo $VERSION
cd $RT_DEST_DIR
echo " - Resetting existing $RT_DEST_DIR"
export GIT_WORK_TREE="$RT_DEST_DIR"
git reset HEAD --hard > /dev/null
git clean -fd > /dev/null
echo " - Removing and recreating $RT_DEST_DIR/$RELEASE"
rm -rf $RT_DEST_DIR/$RELEASE
mkdir $RT_DEST_DIR/$RELEASE
cd $cwd
echo " - Copying devrel to $RT_DEST_DIR/$RELEASE"
cp -p -P -R dev $RT_DEST_DIR/$RELEASE
echo " - Writing $RT_DEST_DIR/$RELEASE/VERSION"
echo -n $VERSION > $RT_DEST_DIR/$RELEASE/VERSION
cd $RT_DEST_DIR
echo " - Reinitializing git state"
git add .
git commit -a -m "riak_test init" --amend > /dev/null
