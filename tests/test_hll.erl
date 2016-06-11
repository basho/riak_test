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
%%% @doc r_t to verify changes to serialization/metadata/changes
%%       across dt upgrades

-module(test_hll).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HLL_TYPE1, <<"hlls1">>).
-define(HLL_TYPE2, <<"hlls2">>).
-define(BUCKET1, {?HLL_TYPE1, <<"testbucket1">>}).
-define(BUCKET2, {?HLL_TYPE2, <<"testbucket2">>}).
-define(DEFAULT_P, 14).
-define(SET_P, 16).
-define(P_SETTING, hll_precision).
-define(KEY, <<"flipit&reverseit">>).
-define(CONFIG,
        [
         {riak_core,
          [{ring_creation_size, 8},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8}]
         }
        ]).

confirm() ->
    %% Configure cluster.
    Nodes = rt:build_cluster(4, ?CONFIG),

    NodeRand1 = rt:select_random(Nodes),
    NodeRand2 = rt:select_random(Nodes),

    %% Create PB connection.
    PBC = rt:pbc(NodeRand1),
    riakc_pb_socket:set_options(PBC, [queue_if_disconnected]),

    %% Create HTTPC connection.
    HttpC = rt:httpc(NodeRand2),

    rt:create_and_activate_bucket_type(NodeRand1,
                                       ?HLL_TYPE1,
                                       [{datatype, hll},
                                        {hll_precision, ?SET_P}]),

    rt:create_and_activate_bucket_type(NodeRand1,
                                       ?HLL_TYPE2,
                                       [{datatype, hll}]),

    pb_tests(PBC, riakc_pb_socket, ?BUCKET1, Nodes),
    http_tests(HttpC, rhc, ?BUCKET2, Nodes),

    %% Stop PB connection.
    riakc_pb_socket:stop(PBC),

    %% Clean cluster
    rt:clean_cluster(Nodes),

    pass.

http_tests(C, CMod, Bucket, Nodes) ->
    lager:info("HTTP CLI TESTS: Create new Hll DT"),
    S0 = riakc_hll:new(),

    add_tests(C, CMod, Bucket, S0),

    HllSet0 = get_hll(C, CMod, Bucket),
    check_precision_and_reduce_test(C, CMod, Bucket, ?DEFAULT_P, HllSet0),

    partition_write_heal(C, CMod, Bucket, Nodes),

    HllSet1 = get_hll(C, CMod, Bucket),
    check_precision_and_reduce_invalid_test(C, CMod, Bucket, ?DEFAULT_P - 1,
                                       HllSet1),

    ok.

pb_tests(C, CMod, Bucket, Nodes) ->
    lager:info("PB CLI TESTS: Create new Hll DT"),
    S0 = riakc_hll:new(),

    add_tests(C, CMod, Bucket, S0),

    HllSet0 = get_hll(C, CMod, Bucket),
    check_precision_and_reduce_test(C, CMod, Bucket, ?SET_P, HllSet0),

    partition_write_heal(C, CMod, Bucket, Nodes),

    HllSet1 = get_hll(C, CMod, Bucket),
    check_precision_and_reduce_invalid_test(C, CMod, Bucket, ?SET_P - 1, HllSet1),

    ok.

add_tests(C, CMod, Bucket, NewS) ->
    add_element(C, CMod, Bucket, NewS, <<"OH">>),
    {ok, S1} = CMod:fetch_type(C, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S1), 1),

    add_elements(C, CMod, Bucket, S1, [<<"C">>, <<"A">>, <<"P">>]),
    {ok, S2} = CMod:fetch_type(C, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S2), 4),

    add_redundant_element(C, CMod, Bucket, S2, <<"OH">>),
    {ok, S3} = CMod:fetch_type(C, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S3), 4).

partition_write_heal(C, CMod, Bucket, Nodes) ->
    lager:info("Partition cluster in two to force merge."),
    [N1, N2, N3, N4] = Nodes,
    PartInfo = rt:partition([N1, N2], [N3, N4]),

    lager:info("Write to one side of the partition"),
    {ok, S0} = CMod:fetch_type(C, Bucket, ?KEY),
    add_element(C, CMod, Bucket, S0, <<"OH hello there">>),
    {ok, S1} = CMod:fetch_type(C, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S1), 5),

    lager:info("Heal"),
    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, riak_kv).

get_hll(C, CMod, Bucket) ->
    {ok, Obj} =CMod:get(C, Bucket, ?KEY),
    {ok, CRDT} = riak_kv_crdt:from_binary(riakc_obj:get_value(Obj)),
    {_, _, _, HllSet} = CRDT,
    HllSet.

add_element(C, CMod, Bucket, S, Elem) ->
    lager:info("Add element to HLL DT"),
    CMod:update_type(
      C, Bucket, ?KEY,
      riakc_hll:to_op(
        riakc_hll:add_element(Elem, S))).

add_elements(C, CMod, Bucket, S, Elems) ->
    lager:info("Add multiple elements to HLL DT"),
    CMod:update_type(
      C, Bucket, ?KEY,
      riakc_hll:to_op(
        riakc_hll:add_elements(Elems, S))).

add_redundant_element(C, CMod, Bucket, S, Elem) ->
    lager:info("Add redundant element to HLL DT by calling"
               " add_element/3 again"),
    add_element(C, CMod, Bucket, S, Elem).

check_precision_and_reduce_test(C, CMod, Bucket, ExpP, HllSet) ->
    {ok, Props0} = CMod:get_bucket(C, Bucket),
    ?assertEqual(proplists:get_value(?P_SETTING, Props0), ExpP),
    ?assertEqual(riak_dt_hll:precision(HllSet), ExpP),
    ok = CMod:set_bucket(C, Bucket, [{?P_SETTING, ExpP - 1}]),
    {ok, Props1} = CMod:get_bucket(C, Bucket),
    ?assertEqual(proplists:get_value(?P_SETTING, Props1), ExpP - 1).

check_precision_and_reduce_invalid_test(C, CMod, Bucket, ExpP, HllSet) ->
    ?assertEqual(riak_dt_hll:precision(HllSet), ExpP),
    {error, _} = CMod:set_bucket(C, Bucket, [{?P_SETTING, ExpP + 1}]),
    {ok, Props} = CMod:get_bucket(C, Bucket),
    ?assertEqual(proplists:get_value(?P_SETTING, Props), ExpP),
    ?assertEqual(riak_dt_hll:precision(HllSet), ExpP).
