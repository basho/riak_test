%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(verify_2i_delete).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").
-import(secondary_index_tests, [put_an_object/2, put_an_object/4, int_to_key/1,
                                stream_pb/3, http_query/3, pb_query/3]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).
-define(EXCHANGE_TICK, 10 * 1000). % Must be > inactivity timeout
-define(DELETE_TIMEOUT, 5000).

-define(CFG(SuspendAAE, DeleteMode), 
    [{riak_core,
        [{ring_creation_size, 16},
        {vnode_inactivity_timeout, 5 * 1000},
        {handoff_timeout, 30 * 1000},
        {handoff_receive_timeout, 30 * 1000},
        {handoff_acklog_threshold, 4},
        {handoff_batch_threshold_count, 500}]},
    {riak_kv, 
        [{anti_entropy, {off, []}},
        {tictacaae_active, active},
        {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
        {tictacaae_exchangetick, ?EXCHANGE_TICK},
        {tictacaae_suspend, SuspendAAE},
        {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
        {tictacaae_primaryonly, true},
        {delete_mode, DeleteMode},
        {handoff_deletes, true}
    ]
    }]).

confirm() ->

    NodesK = rt:build_cluster(4, ?CFG(true, keep)),
    
    test_keepmode_keeps(NodesK),
    
    rt:clean_cluster(NodesK),

    NodesT = rt:build_cluster(4, ?CFG(true, ?DELETE_TIMEOUT)),

    test_deletemode_cleans(NodesT, 50000).


test_keepmode_keeps(Nodes) ->
    
    lager:info("Test $key index in keep mode"),
    lager:info(
        "The $key index does not discriminate tombstone from object ..."),
    lager:info(
        "... so deleting an object will not change results from %key query"),

    RiakHttp = rt:httpc(hd(Nodes)),
    PBC = rt:pbc(hd(Nodes)),

    [put_an_object(PBC, N) || N <- lists:seq(0, 100)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 100)]),
    
    HttpRes =
        rhc:get_index(
            RiakHttp, ?BUCKET, <<"$key">>, {int_to_key(0), int_to_key(999)}),
    {ok, ?INDEX_RESULTS{keys=HttpResKeys}} = HttpRes,

    ?assertMatch(ExpectedKeys, lists:sort(HttpResKeys)),

    ?assertMatch(ok, riakc_pb_socket:delete(PBC, ?BUCKET, int_to_key(0))),

    HttpResD0 =
        rhc:get_index(
            RiakHttp, ?BUCKET, <<"$key">>, {int_to_key(0), int_to_key(999)}),
    {ok, ?INDEX_RESULTS{keys=HttpResKeysD0}} = HttpResD0,


    ?assertMatch(ExpectedKeys, lists:sort(HttpResKeysD0)),

    ?assertMatch(ok, riakc_pb_socket:delete(PBC, ?BUCKET, int_to_key(1))),

    HttpResD1 =
        rhc:get_index(
            RiakHttp, ?BUCKET, <<"$key">>, {int_to_key(0), int_to_key(999)}),
    {ok, ?INDEX_RESULTS{keys=HttpResKeysD1}} = HttpResD1,

    ?assertMatch(ExpectedKeys, lists:sort(HttpResKeysD1)),

    lager:info("Double-checked deleting leaves a tombstone which is on $key"),

    timer:sleep(?DELETE_TIMEOUT + 1000),

    HttpResD2 =
        rhc:get_index(
            RiakHttp, ?BUCKET, <<"$key">>, {int_to_key(0), int_to_key(999)}),
    {ok, ?INDEX_RESULTS{keys=HttpResKeysD2}} = HttpResD2,

    ?assertMatch(ExpectedKeys, lists:sort(HttpResKeysD2)),

    pass.



test_deletemode_cleans(Nodes, KeyCount) ->

    lager:info("Testing $key index with delayed reap"),
    lager:info("Deleting an object will now change the results after timeout"),

    RiakHttp = rt:httpc(hd(Nodes)),
    PBC = rt:pbc(hd(Nodes)),

    [put_an_object(PBC, N) || N <- lists:seq(1, KeyCount)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(2, KeyCount)]),

    ?assertMatch(ok, riakc_pb_socket:delete(PBC, ?BUCKET, int_to_key(1))),
    
    lager:info("Deleted a key - after timeout should not be returned"),

    timer:sleep(?DELETE_TIMEOUT + 1000),

    HttpResD0 =
        rhc:get_index(
            RiakHttp, ?BUCKET, <<"$key">>, {int_to_key(1), int_to_key(KeyCount)}),
    {ok, ?INDEX_RESULTS{keys=HttpResKeysD0}} = HttpResD0,

    lager:info("Should have one less key than was added"),
    ?assertMatch(KeyCount, length(HttpResKeysD0) + 1),

    ?assertMatch(ExpectedKeys, lists:sort(HttpResKeysD0)),

    lists:foreach(
        fun(I) ->
            ok = riakc_pb_socket:delete(PBC, ?BUCKET, int_to_key(I))
        end,
        lists:seq(2, KeyCount)
    ),

    timer:sleep(?DELETE_TIMEOUT + 1000),

    HttpResD1 =
        rhc:get_index(
            RiakHttp, ?BUCKET, <<"$key">>, {int_to_key(1), int_to_key(KeyCount)}),
    {ok, ?INDEX_RESULTS{keys=HttpResKeysD1}} = HttpResD1,

    lager:info("All keys deleted ~p", [HttpResKeysD1]),
    ?assertMatch(0, length(HttpResKeysD1)),

    pass.
    
