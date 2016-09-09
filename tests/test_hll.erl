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
%%% @doc r_t to test hll datatypes across a riak cluster

-module(test_hll).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HLL_TYPE1, <<"hlls1">>).
-define(HLL_TYPE2, <<"hlls2">>).
-define(HLL_TYPE3, <<"hlls3">>).
-define(BUCKET1, {?HLL_TYPE1, <<"testbucket1">>}).
-define(BUCKET2, {?HLL_TYPE2, <<"testbucket2">>}).
-define(DEFAULT_P, 14).
-define(SET_P, 16).
-define(BAD_P, 1).
-define(P_SETTING, hll_precision).
-define(KEY, <<"flipit&reverseit">>).
-define(CONFIG,
        [
         {riak_core,
          [{ring_creation_size, 8},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8},
           {handoff_concurrency, 16}]
         }
        ]).

confirm() ->
    %% Configure cluster.
    Nodes = [N1, N2, N3, N4] = rt:build_cluster(4, ?CONFIG),

    NodeRand1A = rt:select_random([N1, N2]),
    NodeRand1B = rt:select_random([N1, N2]),
    NodeRand2A = rt:select_random([N3, N4]),
    NodeRand2B = rt:select_random([N3, N4]),

    lager:info("Create PB/HTTP Clients from first two nodes, and then"
               " the second two nodes, as we'll partition Nodes 1 & 2 from"
               " Nodes 3 & 4 later"),

    %% Create PB connection.
    PBC1 = rt:pbc(NodeRand1A),
    PBC2 = rt:pbc(NodeRand2A),
    riakc_pb_socket:set_options(PBC1, [queue_if_disconnected]),
    riakc_pb_socket:set_options(PBC2, [queue_if_disconnected]),

    %% Create HTTPC connection.
    HttpC1 = rt:httpc(NodeRand1B),
    HttpC2 = rt:httpc(NodeRand2B),

    ok = rt:create_activate_and_wait_for_bucket_type(Nodes,
                                                     ?HLL_TYPE1,
                                                     [{datatype, hll},
                                                      {?P_SETTING, ?SET_P}]),

    ok = rt:create_activate_and_wait_for_bucket_type(Nodes,
                                                     ?HLL_TYPE2,
                                                     [{datatype, hll}]),

    lager:info("Create a bucket-type w/ a HLL datatype and a bad HLL precision"
               " - This should throw an error"),
    ?assertError({badmatch, {error, [{hll_precision, _}]}},
                 rt:create_activate_and_wait_for_bucket_type(Nodes,
                                                             ?HLL_TYPE3,
                                                             [{datatype, hll},
                                                              {?P_SETTING,
                                                               ?BAD_P}])),

    pb_tests(PBC1, PBC2, riakc_pb_socket, ?BUCKET1, Nodes),
    http_tests(HttpC1, HttpC2, rhc, ?BUCKET2, Nodes),

    %% Stop PB connections.
    riakc_pb_socket:stop(PBC1),
    riakc_pb_socket:stop(PBC2),

    pass.

http_tests(C1, C2, CMod, Bucket, Nodes) ->
    lager:info("HTTP CLI TESTS: Create new Hll DT"),

    add_tests(C1, CMod, Bucket),

    HllSet0 = get_hll(C1, CMod, Bucket),
    check_precision_and_reduce_test(C1, CMod, Bucket, ?DEFAULT_P, HllSet0),

    partition_write_heal(C1, C2, CMod, Bucket, Nodes),

    HllSet1 = get_hll(C1, CMod, Bucket),
    check_precision_and_reduce_invalid_test(C1, CMod, Bucket, ?DEFAULT_P - 1,
                                            HllSet1),

    ok.

pb_tests(C1, C2, CMod, Bucket, Nodes) ->
    lager:info("PB CLI TESTS: Create new Hll DT"),

    add_tests(C1, CMod, Bucket),

    HllSet0 = get_hll(C1, CMod, Bucket),
    check_precision_and_reduce_test(C1, CMod, Bucket, ?SET_P, HllSet0),

    partition_write_heal(C1, C2, CMod, Bucket, Nodes),

    HllSet1 = get_hll(C1, CMod, Bucket),
    check_precision_and_reduce_invalid_test(C1, CMod, Bucket, ?SET_P - 1, HllSet1),

    ok.

add_tests(C, CMod, Bucket) ->
    S0 = riakc_hll:new(),

    add_element(C, CMod, Bucket, S0, <<"OH">>),
    {ok, S1} = CMod:fetch_type(C, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S1), 1),

    add_elements(C, CMod, Bucket, S1, [<<"C">>, <<"A">>, <<"P">>]),
    {ok, S2} = CMod:fetch_type(C, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S2), 4),

    add_redundant_element(C, CMod, Bucket, S2, <<"OH">>),
    {ok, S3} = CMod:fetch_type(C, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S3), 4).

partition_write_heal(C1, C2, CMod, Bucket, Nodes) ->
    lager:info("Partition cluster in two to force merge."),
    [N1, N2, N3, N4] = Nodes,
    PartInfo = rt:partition([N1, N2], [N3, N4]),

    try
        lager:info("Write to one side of the partition"),
        {ok, S0} = CMod:fetch_type(C1, Bucket, ?KEY),
        add_element(C1, CMod, Bucket, S0, <<"OH hello there">>),
        {ok, S1} = CMod:fetch_type(C1, Bucket, ?KEY),
        ?assertEqual(riakc_hll:value(S1), 5),

        lager:info("Write to the other side of the partition"),
        {ok, S2} = CMod:fetch_type(C2, Bucket, ?KEY),
        add_element(C2, CMod, Bucket, S2, <<"Riak 1.4.eva">>),
        {ok, S3} = CMod:fetch_type(C2, Bucket, ?KEY),
        ?assertEqual(riakc_hll:value(S3), 5),

        lager:info("Heal")
    after
        ok = rt:heal(PartInfo)
    end,

    ok = rt:wait_until_no_pending_changes(Nodes),
    ok = rt:wait_until_transfers_complete(Nodes),

    lager:info("Once healed, check both sides for the correct, merged value"),
    {ok, S4} = CMod:fetch_type(C1, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S4), 6),
    {ok, S5} = CMod:fetch_type(C2, Bucket, ?KEY),
    ?assertEqual(riakc_hll:value(S5), 6).

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
    ?assertEqual(riak_kv_hll:precision(HllSet), ExpP),
    ok = CMod:set_bucket(C, Bucket, [{?P_SETTING, ExpP - 1}]),
    {ok, Props1} = CMod:get_bucket(C, Bucket),
    ?assertEqual(proplists:get_value(?P_SETTING, Props1), ExpP - 1).

check_precision_and_reduce_invalid_test(C, CMod, Bucket, ExpP, HllSet) ->
    lager:info("HLL's can be reduced, but never increased.\n"
               " Test to make sure we don't allow invalid values."),

    ?assertEqual(riak_kv_hll:precision(HllSet), ExpP),
    {error, _} = CMod:set_bucket(C, Bucket, [{?P_SETTING, ExpP + 1}]),
    {ok, Props} = CMod:get_bucket(C, Bucket),
    ?assertEqual(proplists:get_value(?P_SETTING, Props), ExpP),
    ?assertEqual(riak_kv_hll:precision(HllSet), ExpP).
