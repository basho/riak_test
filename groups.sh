#!/usr/bin/env bash

# $1 is backend $2 is riak test profile

set -e

echo "making results dir"

BASE_DIR=$(date +"%FT%H%M")-$BACKEND
mkdir -p $BASE_DIR

for group in groups/*; do  ./group.sh $(basename $group) $1 $2 $BASE_DIR; done
