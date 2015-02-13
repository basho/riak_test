#!/usr/bin/env bash

# Bail out on error ...
set -e

# Stash the current directory before doing any work so we can return back to where we started ...
CURRENT_DIR=`pwd`

# Determine the location of script which will allow to determine the riak_test home ...
# Borrowed liberally from http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done

RT_BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
RT_HOME="$( dirname "$RT_BIN_DIR" )"
FULL_CLEAN=false
VERSION="2.0"
NUM_NODES=5

usage() {
  echo "Resets the current riak_test environment by rebuilding riak and riak_test using rtdev-install.sh"
  echo "  -c: Perform a devclean on the riak and clean on riak_test projects (default: $FULL_CLEAN)"
  echo "  -n: Number of nodes on which to test (default: $NUM_NODES)"
  echo "  -v: The Riak version to test.  The Riak home is calculated as $RT_HOME/riak-<version> (default: $VERSION)"
  echo "  -h: This help message"
}

while getopts chn:v: opt; do
  case $opt in
    c) FULL_CLEAN=true
       ;;
    v) VERSION=$OPTARG
       ;;
    n) NUM_NODES=$OPTARG
       ;;
    h) usage
       exit 0
       ;;
  esac
done

shift $(($OPTIND-1))

RIAK_HOME=$RT_HOME/$VERSION

if ! [[ -d $RIAK_HOME || -h $RIAK_HOME ]]; then
  echo "Riak home $RIAK_HOME does not exist."
  exit 1
fi

echo "Resetting the riak_test environment using RIAK_HOME=$RIAK_HOME, RT_HOME=$RT_HOME, NUM_NODES=$NUM_NODES, VERSION=$VERSION, and FULL_CLEAN=$FULL_CLEAN"

cd $RIAK_HOME
rm -rf current

# Clean out deps for when a full clean is requested ...
if [ "$FULL_CLEAN" = true ] ; then
  echo "Cleaning Riak in $RIAK_HOME ..."
  make distclean
fi

echo "Removing previous stagedevrel instance from $RIAK_HOME and rebuilding ..."
make devclean

# Rebuild Riak ...
echo "Building Riak stagedevrel with $NUM_NODES nodes in $RIAK_HOME ..."
make stagedevrel DEVNODES=$NUM_NODES

$RT_HOME/bin/rtdev-install.sh

cd $RT_HOME

if [ "$FULL_CLEAN" = true ] ; then
  echo "Cleaning riak_test in $RT_HOME ..."
  make distclean
fi

echo "Rebuilding riak_test in $RT_HOME ..."
make

# Return back to where we started ...
cd $CURRENT_DIR

exit 0

