#!/bin/bash

# Keys Count
keys1=$(expr `curl -XGET $BUCKET_URL 2>/dev/null | json_pp | wc -l` - 4)

# Documents Count
docs1=`curl -XGET $SEARCH_URL 2>/dev/null | json_pp | grep numFound | sed "s/[^0-9]//g"`

# Replicas Count
reps1=`curl -XGET $SOLR_URL_BEFORE 2>/dev/null | json_pp | grep numFound | sed "s/[^0-9]//g"`

# Leave Node 1
$ADMIN_PATH_NODE1 cluster leave &> /dev/null
$ADMIN_PATH_NODE1 cluster plan &> /dev/null
$ADMIN_PATH_NODE1 cluster commit &> /dev/null

sleep 10

# Keys Count
keys2=$(expr `curl -XGET $BUCKET_URL 2>/dev/null | json_pp | wc -l` - 4)

# Documents Count
docs2=`curl -XGET $SEARCH_URL 2>/dev/null | json_pp | grep numFound | sed "s/[^0-9]//g"`

# Replicas Count
reps2=`curl -XGET $SOLR_URL_AFTER 2>/dev/null | json_pp | grep numFound | sed "s/[^0-9]//g"`

echo "keys1" $keys1
echo "keys2" $keys2
echo "docs1 "$docs1
echo "docs2" $docs2
echo "reps1" $reps1
echo "reps2" $reps2
