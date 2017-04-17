%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.
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

-module(verify_dt_data_upgrade).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(MAP_TYPE, <<"maps">>).
-define(SET_TYPE, <<"sets">>).
-define(SET_CAP, riak_dt_orswot).
-define(BUCKET_M, {?MAP_TYPE, <<"testbucket">>}).
-define(BUCKET_S, {?SET_TYPE, <<"testbucket">>}).
-define(KEY, <<"flipit&reverseit">>).
-define(CONFIG, [
    {riak_core, [
        {ring_creation_size, 8},
        %% shorten cluster convergence
        {vnode_parallel_start, 8},
        {forced_ownership_handoff, 8},
        {handoff_concurrency, 8},
        %% increase AAE activity
        {anti_entropy_build_limit, {100, 1000}},
        {anti_entropy_concurrency, 8}
    ]}
]).

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, legacy),

    NumNodes = 4,
    Vsns = [{OldVsn, ?CONFIG} || _ <- lists:seq(1, NumNodes)],

    Nodes = [Node1,Node2|Nodes34] = rt:build_cluster(Vsns),
    rt:wait_until_transfers_complete(Nodes),

    %% Create PB connection.
    {P1, P2} = lists:split(1, Nodes),

    rt:create_activate_and_wait_for_bucket_type(Nodes,
                                                ?SET_TYPE,
                                                [{datatype, set}]),
    rt:create_activate_and_wait_for_bucket_type(Nodes,
                                                ?MAP_TYPE,
                                                [{datatype, map}]),

    %% Partition the Cluster into 2 Sides
    Partition = rt:partition(P1, P2),

    Pid1 = rt:pbc(hd(P1)),
    Pid2 = rt:pbc(hd(P2)),

    riakc_pb_socket:set_options(Pid1, [queue_if_disconnected, auto_reconnect]),
    riakc_pb_socket:set_options(Pid2, [queue_if_disconnected, auto_reconnect]),

    lager:info("Writing to Partition 1 Key ~p ", [?KEY]),
    make_and_check(Pid1, [a, b]),

    %% Upgrade Nodes 3 - 4 only
    upgrade(Nodes34, current),

    lager:info("Writing to Partition 2 Key W/ Different Set ~p ", [?KEY]),
    make_and_check(Pid2, [c, d, e]),

    rt:heal(Partition),
    rt:wait_until_transfers_complete(Nodes),

    lager:info("Compare/Assert fetched values merge/remain as supposed to"
               ", including binary information"),

    FetchSet0 = fetch(Pid1, ?BUCKET_S, ?KEY),
    FetchMap0 = fetch(Pid1, ?BUCKET_M, ?KEY),

    FetchSet1 = fetch(Pid2, ?BUCKET_S, ?KEY),
    FetchMap1 = fetch(Pid2, ?BUCKET_M, ?KEY),

    ?assertEqual(FetchMap0, FetchMap1),
    ?assertEqual(FetchSet0, FetchSet1),

    verify_dt_converge:check_value(Pid1, riakc_pb_socket,
                                   ?BUCKET_S, ?KEY, riakc_set,
                                   [<<"a">>, <<"b">>, <<"c">>, <<"d">>,
                                    <<"e">>]),

    %% Upgrade Nodes 1-2
    upgrade([Node1, Node2], current),

    FetchSet2 = fetch(Pid1, ?BUCKET_S, ?KEY),
    FetchMap2 = fetch(Pid1, ?BUCKET_M, ?KEY),

    FetchSet3 = fetch(Pid2, ?BUCKET_S, ?KEY),
    FetchMap3 = fetch(Pid2, ?BUCKET_M, ?KEY),

    ?assertEqual(FetchMap2, FetchMap3),
    ?assertEqual(FetchSet2, FetchSet3),

    %% Downgrade All Nodes and Compare
    downgrade(Nodes, legacy),

    %% Create PB connection.
    Pid3 = rt:pbc(rt:select_random(Nodes)),

    FetchSet4 = fetch(Pid3, ?BUCKET_S, ?KEY),
    FetchMap4 = fetch(Pid3, ?BUCKET_M, ?KEY),
    ?assertEqual(FetchMap0, FetchMap4),
    ?assertEqual(FetchSet3, FetchSet4),

    %% Stop PB connection.
    riakc_pb_socket:stop(Pid1),
    riakc_pb_socket:stop(Pid2),
    riakc_pb_socket:stop(Pid3),

    %% Clean cluster.
    rt:clean_cluster(Nodes),

    pass.

%% private funs

store_set(Client, Bucket, Key, Set) ->
    riakc_pb_socket:update_type(Client, Bucket, Key, riakc_set:to_op(Set)).

store_map(Client, Bucket, Key, Map) ->
    riakc_pb_socket:update_type(Client, Bucket, Key, riakc_map:to_op(Map)).

fetch(Client, Bucket, Key) ->
    {ok, DT} = riakc_pb_socket:fetch_type(Client, Bucket, Key),
    DT.

upgrade(Nodes, NewVsn) ->
    lager:info("Upgrading to ~s version on Nodes ~p", [NewVsn, Nodes]),
    [rt:upgrade(ANode, NewVsn) || ANode <- Nodes],
    [rt:wait_for_service(ANode, riak_kv) || ANode <- Nodes],
    [rt:wait_until_capability_contains(ANode, {riak_kv, crdt}, [?SET_CAP])
     || ANode <- Nodes].

downgrade(Nodes, OldVsn) ->
    lager:info("Downgrading to ~s version on Nodes ~p", [OldVsn, Nodes]),
    [rt:upgrade(ANode, OldVsn) || ANode <- Nodes],
    [rt:wait_for_service(ANode, riak_kv) || ANode <- Nodes],
    [rt:wait_until_capability_contains(ANode, {riak_kv, crdt}, [?SET_CAP])
     || ANode <- Nodes].

make_and_check(Pid, SetItems) ->
    Set0 = verify_dt_context:make_set(SetItems),
    ok = store_set(Pid, ?BUCKET_S, ?KEY, Set0),

    Set1 = verify_dt_context:make_set([mtv, raps]),
    Set2 = verify_dt_context:make_set([x, y, z]),

    Map0 = verify_dt_context:make_map([{<<"set1">>, Set1}, {<<"set2">>, Set2}]),
    ok = store_map(Pid, ?BUCKET_M, ?KEY, Map0),

    CheckSet = [atom_to_binary(E, latin1) || E <- SetItems],

    verify_dt_converge:check_value(Pid, riakc_pb_socket,
                                   ?BUCKET_S, ?KEY, riakc_set,
                                   CheckSet),

    verify_dt_converge:check_value(Pid, riakc_pb_socket,
                                   ?BUCKET_M, ?KEY, riakc_map,
                                   [{{<<"set1">>, set}, [<<"mtv">>,
                                                         <<"raps">>]},
                                    {{<<"set2">>, set}, [<<"x">>, <<"y">>,
                                                         <<"z">>]}]).
