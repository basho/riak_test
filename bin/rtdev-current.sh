#!/usr/bin/env bash

# just bail out if things go south
set -e

: ${RT_DEST_DIR:="$HOME/rt/riak"}
# If RT_CURRENT_TAG is specified, it will use that version number
# otherwise the last annotated tag will be used
: ${RT_CURRENT_TAG:=""}

echo "Making $(pwd) a tagged release:"
cwd=$(pwd)
echo -n " - Determining version: "
if [ -n "$RT_CURRENT_TAG" ]; then
	VERSION=$RT_CURRENT_TAG
elif [ -f $cwd/dependency_manifest.git ]; then
    VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
else
    VERSION=`git describe --tags | awk '{sub(/riak-/,"",$0);print}'`
fi
echo $VERSION
cd $RT_DEST_DIR
echo " - Resetting existing $RT_DEST_DIR"
git reset HEAD --hard > /dev/null 2>&1
git clean -fd > /dev/null 2>&1
echo " - Removing and recreating $RT_DEST_DIR/$VERSION"
rm -rf $RT_DEST_DIR/$VERSION
mkdir $RT_DEST_DIR/$VERSION
cd $cwd
echo " - Copying devrel to $RT_DEST_DIR/$VERSION"
cd dev
for i in `ls`; do cp -p -P -R $i $RT_DEST_DIR/$VERSION/; done
echo " - Writing $RT_DEST_DIR/$VERSION/VERSION"
echo -n $VERSION > $RT_DEST_DIR/$VERSION/VERSION
cd $RT_DEST_DIR
echo " - Reinitializing git state"
git add -f .
git commit -a -m "riak_test init" --amend > /dev/null 2>&1
