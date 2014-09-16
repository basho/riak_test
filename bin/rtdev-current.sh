#!/usr/bin/env bash

# bail out if things go south
set -e

: ${RT_DEST_DIR:="$HOME/rt/riak"}
# If RT_CURRENT_TAG is specified, it will use that version number
# otherwise the last annotated tag will be used
: ${RT_CURRENT_TAG:=""}

echo "Making $(pwd) the current release:"
cwd=$(pwd)
echo -n " - Determining version: "
if [ -n "$RT_CURRENT_TAG" ]; then
	VERSION=$RT_CURRENT_TAG
elif [ -f $cwd/dependency_manifest.git ]; then
    VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
else
    VERSION="$(git describe --tags)-$(git branch | awk '/\*/ {print $2}')"
fi
echo $VERSION
cd $RT_DEST_DIR
echo " - Resetting existing $RT_DEST_DIR"
git reset HEAD --hard
git clean -fd
echo " - Removing and recreating $RT_DEST_DIR/current"
rm -rf $RT_DEST_DIR/current
mkdir $RT_DEST_DIR/current
cd $cwd
echo " - Copying devrel to $RT_DEST_DIR/current"
cp -p -P -R dev $RT_DEST_DIR/current
echo " - Writing $RT_DEST_DIR/current/VERSION"
echo -n $VERSION > $RT_DEST_DIR/current/VERSION
cd $RT_DEST_DIR
echo " - Reinitializing git state"
git add .
git commit -a -m "riak_test init" --amend
