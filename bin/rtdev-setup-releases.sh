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
        # Route out the product and version from Git
        (cd "$rel"; VERSION="$(git describe --tags)"; echo -n $VERSION > $RT_DEST_DIR/$vsn/VERSION)
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

if [ -z "$(git config --list 2>&1 | grep unknown)" ]; then
    git config --local user.name "Riak Test"
    git config --local user.email "dev@basho.com"
    git config --local core.autocrlf input
    git config --local core.safecrlf false
    git config --local core.filemode true
fi

## this prevents priv/*.so files from being deleted by git clean -fd
## (the latter is executed in rtdev-current.sh):
echo "priv/" >.gitignore

git add --all --force .
git commit -a -m "riak_test init" > /dev/null
echo " - Successfully completed initial git commit of $RT_DEST_DIR"
