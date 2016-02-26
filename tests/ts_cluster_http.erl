%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc A module to test riak_ts basic create bucket/put/select cycle
%%      using http client (rhc_ts).

-module(ts_cluster_http).
-behavior(riak_test).
-export([confirm/0]).

%% inject characters to check transit of keys over http barrier
%% (involves urlendofing/decoding of field values)
-define(PVAL_P1, <<"ZXC1/1">>).
-define(PVAL_P2, <<"PDP -11">>).

confirm() ->
    ts_cluster_comprehensive:run_tests(?PVAL_P1, ?PVAL_P2, rhc_ts).

%% below is an alternative test scenario, using curl:
%% #!/bin/bash
%%
%% set -e
%%
%% extra_opts=$*
%% host=http://localhost:10018
%%
%% ddl="create table bob (a varchar not null, b varchar not null, c timestamp not null, d sint64, primary key ((a, b, quantum(c, 1, m)), a, b, c))"
%%
%% function req {
%%     local expected="$5" payload="$4" method_option="-X $2"
%%     echo -n "$1 ($2) ..."
%%     got=`curl $method_option $host/ts/v1/$3 -s $extra_opts \
%%          --data "$payload"`
%%     if ! [[ $got =~ $expected ]]; then
%%         echo
%%         echo "Request failed: $2 $3 $4:"
%%         echo "Expected output: $expected"
%%         echo "            Got: $got"
%%         return 1
%%     else
%%         echo " ok"
%%     fi
%% }
%%
%% req "create table" \
%%     POST query "{\"query\": \"$ddl\"}" \
%%     "ok|already_active" || :
%%
%% req "describe table" \
%%     GET  query '{"query": "describe bob"}' \
%%     columns
%%     #'{\"columns\":[\"Column\",\"Type\",\"Is Null\",\"Primary Key\",\"Local Key\"],\"rows\":[[\"a\",\"varchar\",false,1,1],[\"b\",\"varchar\",false,2,2],[\"c\",\"timestamp\",false,3,3],[\"d\",\"sint64\",true,\"undefined\",\"undefined\"]]}'
%%
%% #echo "sleeping to allow riak to complete table activation"
%% #sleep 4
%%
%% req "put some data" \
%%     PUT  tables/bob/keys '{"data": [["q1", "w1", 11, 110], ["q1", "w1", 22, 220], ["q2", "w2", 22, 220]]}' \
%%     "ok"
%%
%% req "single-key get (field-qualified)" \
%%     GET  tables/bob/keys/a/q1/b/w1/c/11 '' \
%%     columns
%%     #'{"columns":["a","b","c","d"],"rows":[["q1","w1",11,110]]}'
%%
%% req "single-key get (field-qualified, shuffled)" \
%%     GET  tables/bob/keys/b/w1/a/q1/c/11 '' \
%%     columns
%%     #'{"columns":["a","b","c","d"],"rows":[["q1","w1",11,110]]}'
%%
%% req "single-key get (bare values)" \
%%     GET  tables/bob/keys/q1/w1/11 '' \
%%     columns
%%     #'{"columns":["a","b","c","d"],"rows":[["q1","w1",11,110]]}'
%%
%% req "single-key non-existing get" \
%%     GET  tables/bob/keys/no/such/333 '' \
%%     "Key not found"
%%
%% req "list_keys" \
%%     GET  tables/bob/list_keys '' \
%%     keys
%%     #'{"keys":[["q2","w2",22]]}{"keys":[["q1","w1",22],["q1","w1",11]]}{"keys":[]}'
%%
%% req "select query" \
%%     GET  query "{\"query\": \"select * from bob where a = 'q1' and b = 'w1' and c > 1 and c < 999\"}" \
%%     columns
%%     #'{"columns":["a","b","c","d"],"rows":[["q1","w1",11,110],["q1","w1",22,220]]}'
%%
%% req "single-key delete" \
%%     DELETE tables/bob/keys/q1/w1/22 '' \
%%     "ok"
%%
%% req "select query (one record missing)" \
%%     GET  query "{\"query\": \"select * from bob where a = 'q1' and b = 'w1' and c > 1 and c < 999\"}" \
%%     columns
%%     #'{"columns":["a","b","c","d"],"rows":[["q1","w1",11,110]]}'
%%
%% req "select query (empty)" \
%%     GET  query "{\"query\": \"select * from bob where a = 'q9' and b = 'w1' and c > 1 and c < 999\"}" \
%%     columns
%%     #'{"columns":["a","b","c","d"],"rows":[]}'
%%
%% req "coverage" \
%%     GET coverage "{\"query\": \"select * from bob where a = 'q1' and b = 'w1' and c > 1 and c < 999\"}" \
%%     entry || :  # uncomment it reenabled in riak_kv
%%     #'{"coverage":{"entry":'
%%
%% # some errors:
%%
%% req "bad get (wrong table)" \
%%     GET tables/bib/keys/q1/w1/11 '' \
%%     "does not"
%%     #'Table "bib" does not exist'
%%
%% req "bad put (wrong parameters)" \
%%     PUT tables/bob/keys/p1/w1/11 '' \
%%     "Malformed PUT request"
%%
%% req "bad put (wrong table and parameters)" \
%%     PUT tables/bib/keys '{"key": ["p1", "w1", 11]}' \
%%     "Malformed PUT request"
%%
%% req "bad put (parameters ok but not quite)" \
%%     PUT tables/bob/keys '{"data": [["p1", "w1", 11]]}' \
%%     "Invalid record #1"
%%
%% req "bad get (short key)" \
%%     GET tables/bob/keys/p1/w1 '' \
%%     Unpaired
%%
%% req "bad get (long key)" \
%%     GET tables/bob/keys/p1/w1/w1/w1/w1 '' \
%%     Unpaired
%%
%% req "bad query 1" \
%%     GET query "babad" \
%%     'Malformed GET request'
%% req "bad query 2" \
%%     PUT query "babad" \
%%     'Method Not Allowed'
%% req "bad query 3" \
%%     POST query "babad" \
%%     'Malformed POST request'
%%
%% req "inappropriate query for method (PUT and CREATE TABLE)" \
%%     PUT query "{\"query\": \"$ddl\"}" \
%%     'Method Not Allowed'
%% req "inappropriate query for method (GET and CREATE TABLE)" \
%%     GET query "{\"query\": \"$ddl\"}" \
%%     'Inappropriate method GET for SQL query type'
%% req "inappropriate query for method (PUT and SELECT)" \
%%     PUT  query "{\"query\": \"select * from bob where a = 'q9' and b = 'w1' and c > 1 and c < 999\"}" \
%%     'Method Not Allowed'
