#!/bin/bash

# Keys Count
keys1=$(expr `curl -XGET $BUCKET_URL 2>/dev/null | json_pp | wc -l` - 4)

# Documents Count
docs1=`curl -XGET $SEARCH_URL 2>/dev/null | json_pp | grep numFound | sed "s/[^0-9]//g"`

# Replicas Count
reps1=`curl -XGET $SOLR_URL_BEFORE 2>/dev/null | json_pp | grep numFound | sed "s/[^0-9]//g"`

if [[ $JOIN_NODE && ${JOIN_NODE-x} ]]; then
    $RIAK_PATH_NODE start
    sleep 2
    $ADMIN_PATH_NODE cluster join $JOIN_NODE
    $ADMIN_PATH_NODE cluster plan
    $ADMIN_PATH_NODE cluster commit
else
    # Leave Node
    $ADMIN_PATH_NODE cluster leave
    $ADMIN_PATH_NODE cluster plan
    $ADMIN_PATH_NODE cluster commit
fi

sleep 10

while ! $ADMIN_PATH_NODE2 transfers | grep -iqF "$STOPWORDS"
do
    echo 'Transfers in Progress'
    sleep 10
done

# Keys Count
keys2=$(expr `curl -XGET $BUCKET_URL 2>/dev/null | json_pp | wc -l` - 4)

# Documents Count
docs2=`curl -XGET $SEARCH_URL 2>/dev/null | json_pp | grep numFound | sed "s/[^0-9]//g"`

echo "keys1" $keys1
echo "keys2" $keys2
echo "docs1 "$docs1
echo "docs2" $docs2
echo "reps1" $reps1
echo "reps2URL" $SOLR_URL_AFTER
