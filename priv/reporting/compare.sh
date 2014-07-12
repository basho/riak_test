#!/usr/bin/env bash
set -e

if [ ! -d "$1/" -o ! -d "$2" ]; then
    echo "two directories must be specified"
    exit 1
fi

#digesting the files is expensive, don't do it by default
if [ ! -z $3 -a $3 = "true" ]; then
    #make our digest files
    (cd $1; rm -f *-digest; escript ~/bin/riak-digest.escript)
    (cd $2; rm -f *-digest; escript ~/bin/riak-digest.escript)
fi

D1=`basename "$1"`
D2=`basename "$2"`

#generate our comparison graph
gnuplot -e "dir1=\"$1\"; dir2=\"$2\"; outfilename=\"${D1}vs${D2}-summary.png\";" ~/bin/summarize.gpl
