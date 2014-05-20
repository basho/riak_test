#!/bin/bash -x

# Stash the current directory before doing any work so we can return back to where we started ...
CURRENT_DIR=`pwd`

FULL_CLEAN=false
while getopts c opt; do
  case $opt in
    c) FULL_CLEAN=true
      ;;
  esac
done

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
# TODO Allow RIAK_HOME to overridden by a command line switch ...
RIAK_HOME=$RT_HOME/riak

cd $RIAK_HOME

echo "Removing previous devreal instance from $RIAK_HOME and rebuilding ..."

# Clean out previous devrel build ...
if [ "$FULL_CLEAN" = true ] ; then
  make devclean
else
  rm -rf dev
fi

# Rebuild Riak ...
make stagedevrel

$RT_HOME/bin/rtdev-current.sh

echo "Rebuilding riak_test in $RT_HOME ..."
cd $RT_HOME

if [ "$FULL_CLEAN" = true ] ; then
  make clean
fi
make

# Return back to where we started ...
cd $CURRENT_DIR

