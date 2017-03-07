%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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

-module(ts_verify_w1c_delete).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

%% ------------------------------------------------------------
%% Setup cluster and initialize buckets
%% ------------------------------------------------------------

setup_cluster() ->

    %% Start a 3-node cluster

    Cluster = ts_setup:start_cluster(3),

    %% Create the default TS table

    Table   = ts_data:get_default_bucket(),
    DDL     = ts_data:get_ddl(),
    {ok,_}  = ts_setup:create_bucket_type(Cluster, DDL, Table),
    ok      = ts_setup:activate_bucket_type(Cluster, Table),

    %% Create KV bucket

    KvBucketType = <<"kv_w1c_type">>,
    {ok,_}  = create_write_once_kv_bucket_type(Cluster, KvBucketType, 3),
    ok      = ts_setup:activate_bucket_type(Cluster, KvBucketType),

    {Table, KvBucketType}.

%% ------------------------------------------------------------
%% Create a KV write-once bucket type
%% ------------------------------------------------------------

create_write_once_kv_bucket_type([Node|_Rest], BucketType, NVal) when is_integer(NVal) ->
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": ~s, \\\"write_once\\\": true}}", [integer_to_list(NVal)]),
    rt:admin(Node, ["bucket-type", "create", binary_to_list(BucketType), lists:flatten(Props)]).

%% ------------------------------------------------------------
%% Get client connection to the cluster
%% ------------------------------------------------------------

getClient() ->
    getClient("127.0.0.1", 10017).

getClient(Ip, Port) ->
    {ok, C} = riakc_pb_socket:start_link(Ip, Port),
    C.

%% ------------------------------------------------------------
%% Put a TS key into the default TS table, and verify that it can be
%% read back
%% ------------------------------------------------------------

verify_ts_put_key(Bucket, Ts) ->

    C = getClient(),

    PartList = [<<"family">>, <<"series">>, Ts],
    Data = [list_to_tuple(PartList ++ [<<"weather">>, 1.234])],

    %% Put the key, and verify that it can be read back

    ok = riakc_ts:put(C, Bucket, Data),
    {ok, {_Cols, Data}} = riakc_ts:get(C, Bucket, PartList, []),

    {C, PartList}.

%% ------------------------------------------------------------
%% Put a TS key into the default TS table, delete it via API delete
%% call, and verify that it is gone
%% ------------------------------------------------------------

verify_ts_api_delete_key(Bucket) ->

    %% Put the TS key, and verify that it can be read back

    {C, PartList} = verify_ts_put_key(Bucket, 1),

    %% Delete the key via API delete, and verify that it is gone

    ok = riakc_ts:delete(C, Bucket, PartList, []),
    {ok, {[], []}} = riakc_ts:get(C, Bucket, PartList, []).

%% ------------------------------------------------------------
%% Put a TS key into the default TS table, delete it via SQL query,
%% and verify that it is gone
%% ------------------------------------------------------------

verify_ts_sql_delete_key(Bucket) ->

    %% Put the TS key, and verify that it can be read back

    {C, PartList} = verify_ts_put_key(Bucket, 1),

    [Family, Series, Time] = PartList,
    Query = 
	"delete from " ++ Bucket ++ " where " ++
	" myfamily='"  ++ binary_to_list(Family) ++ "' and " ++
	" myseries='"  ++ binary_to_list(Series) ++ "' and " ++
	" time="       ++ integer_to_list(Time),

    %% Delete the key via SQL query, and verify that it is gone

    {ok, {[], []}} = riakc_ts:query(C, Query),
    {ok, {[], []}} = riakc_ts:get(C, Bucket, PartList, []).

%% ------------------------------------------------------------
%% Put a KV key into the write-once bucket and verify that is can be
%% read back
%% ------------------------------------------------------------

verify_kv_put_key(BucketType) ->

    C    = getClient(),

    BucketName = <<"kv_w1c_bucket">>,
    Key        = <<"kv_w1c_key">>,
    Val        = <<"kv_w1c_val">>,

    CompositeBucket = {BucketType, BucketName},

    Obj = riakc_obj:new(CompositeBucket, Key, Val),

    ok  = riakc_pb_socket:put(C, Obj),
    Ret = riakc_pb_socket:get(C, CompositeBucket, Key),
    {ok, {riakc_obj, CompositeBucket, Key, _Bin, [{_Meta, Val}], undefined, undefined}} = Ret,

    {C, CompositeBucket, Key}.

%% ------------------------------------------------------------
%% Put a KV key into the write-once bucket, then delete it and verify
%% that it is gone
%% ------------------------------------------------------------

verify_kv_api_delete_key(BucketType) ->

    %% Put the KV key, and verify that it can be read back

    {C, CompositeBucket, Key} = verify_kv_put_key(BucketType),

    %% Delete the KV key, and verify that it is gone

    ok = riakc_pb_socket:delete(C, CompositeBucket, Key),
    {error, notfound} = riakc_pb_socket:get(C, CompositeBucket, Key).


%% ============================================================
%% Main test: 
%%
%%    - setup 3-node cluster
%%    - create TS table and write-once KV bucket
%%    - verify all versions of deletes against TS tables
%%    - verify api deletse against write-once KV buckets
%%
%% ============================================================

confirm() ->

    {TsTable, KvBucketType} = setup_cluster(),

    verify_ts_api_delete_key(TsTable),
    verify_ts_sql_delete_key(TsTable),
    verify_kv_api_delete_key(KvBucketType),

    pass.
