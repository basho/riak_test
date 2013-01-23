#!/usr/bin/env bash
: ${RT_DEST_DIR:="$HOME/rt/riak"}

echo "Making $(pwd) the current release:"
cwd=$(pwd)
echo -n " - Determining version: "
if [ -f $cwd/dependency_manifest.git ]; then
    VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
else
    VERSION="$(git describe --tags)-$(git branch | awk '/\*/ {print $2}')"
fi
echo $VERSION
cd $RT_DEST_DIR
echo " - Resetting existing $RT_DEST_DIR"
git reset HEAD --hard > /dev/null 2>&1
git clean -fd > /dev/null 2>&1
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
git commit -a -m "riak_test init" --amend > /dev/null 2>&1
