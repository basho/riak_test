#!/usr/bin/env bash
set -e

if [ ! -d "$1/" ]; then
    echo "a directory must be specified"
    exit 1
fi
if [ "x$2" == "xtrue" ]; then
  (cd $1; rm -f *-digest; escript ../../priv/reporting/riak-digest.escript)
fi

D1=`basename "$1"`

#generate our comparison graph
gnuplot -e "dir1=\"$1\"; outfilename=\"${D1}-report.png\";" priv/reporting/summarize1.gpl
