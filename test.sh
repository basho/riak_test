#!/bin/bash 
while [ $? -eq 0 ]; do
  ./riak_test -t watchmen:always_pass -t watchmen:randomly -o out
done