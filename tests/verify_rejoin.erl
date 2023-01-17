%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(verify_rejoin).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_ITEM_COUNT, 12000).
-define(B, <<"systest">>).
-define(EXCHANGE_TICK, 10 * 1000). % Must be > inactivity timeout

-define(CFG(SuspendAAE), 
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
        {tictacaae_primaryonly, true}]
    }]).

confirm() ->
    %% Bring up a 3-node cluster for the test

    lager:info("Leave and join nodes during write activity"),
    lager:info("Do this with aggressive AAE - but we shouldn't see AAE repairs"),
    lager:info("In the future - need a riak stat for repairs to validate this"),
    Nodes = rt:build_cluster(4, ?CFG(false)),
    [Node1, Node2, Node3, Node4] = Nodes,

    lager:info("Writing ~p items", [?TEST_ITEM_COUNT]),
    [] = rt:systest_write(Node1, 1, ?TEST_ITEM_COUNT, ?B, 2),

    lager:info("Have node2 leave and continue to write"),
    rt:leave(Node2),
    [] = rt:systest_write(
            Node1, ?TEST_ITEM_COUNT + 1, 2 * ?TEST_ITEM_COUNT, ?B, 2),
    ?assertEqual(ok, rt:wait_until_unpingable(Node2)),

    lager:info("Have node3 leave and continue to write"),
    rt:leave(Node3),
    [] = rt:systest_write(
            Node1, 2 * ?TEST_ITEM_COUNT + 1, 3 * ?TEST_ITEM_COUNT, ?B, 2),
    ?assertEqual(ok, rt:wait_until_unpingable(Node3)),
    
    lager:info("Restart node2"),
    rt:start_and_wait(Node2),
    timer:sleep(5000),
    
    lager:info("Rejoin node2 and continue to write"),
    rt:join(Node2, Node1),
    [] = rt:systest_write(
            Node1, 3 * ?TEST_ITEM_COUNT + 1, 4 * ?TEST_ITEM_COUNT, ?B, 2),
    check_joined([Node1, Node2, Node4]),

    lager:info("Restart node3"),
    rt:start_and_wait(Node3),
    timer:sleep(5000),

    lager:info("Rejoin node3 and continue to write"),
    rt:join(Node3, Node1),
    [] = rt:systest_write(
            Node1, 4 * ?TEST_ITEM_COUNT + 1, 5 * ?TEST_ITEM_COUNT, ?B, 2),
    check_joined([Node1, Node2, Node3, Node4]),

    lager:info("Checking for AAE repairs - should be none"),
    timer:sleep(?EXCHANGE_TICK),
    {RCT0, _CCT0} = tictacaae_accumulate_stats(Nodes),
    timer:sleep(?EXCHANGE_TICK),
    {RCT1, _CCT1} = tictacaae_accumulate_stats(Nodes),
    timer:sleep(?EXCHANGE_TICK),
    {RCT2, CCT2} = tictacaae_accumulate_stats(Nodes),

    true = CCT2 == 0,
    true = RCT2 > RCT1,
    true = RCT1 > RCT0,

    lager:info("Check all values read"),
    [] = rt:systest_read(Node1, 5 * ?TEST_ITEM_COUNT),
    
    {0, <<"undefined">>, <<"undefined">>} = repair_stats(Node1),
    {0, <<"undefined">>, <<"undefined">>} = repair_stats(Node2),

    lager:info(
        "Testing without AAE - and varying read repair settings "
        "as well as handoff settings"),
    lager:info("Update config on all nodes"),
    lists:foreach(
        fun(N) -> rt:update_app_config(N, ?CFG(true)) end,
        [Node1, Node2, Node3, Node4]
    ),
    ok = rt:wait_until_no_pending_changes([Node1, Node2, Node3, Node4]),    

    lager:info("Set Node 1 not to do fallback repair"),
    lager:info("Stop Node 3 so read repairs happen on GET"),
    ok =
        rpc:call(
            Node1,
            application,
            set_env,
            [riak_kv, read_repair_primaryonly, true]),
    
    rt:stop_and_wait(Node3),
    ok = rt:wait_until_no_pending_changes([Node1, Node2]),
    ok = rt:wait_until(fun() -> upnode_count(Node1, 3) end),
    ok = rt:wait_until(fun() -> upnode_count(Node2, 3) end),
    timer:sleep(10000),
    lager:info("Read from node1, write some more, read some from node 2"),
    [] =
        rt:systest_read(Node1, 1, ?TEST_ITEM_COUNT * 2, ?B, 2),
    [] =
        rt:systest_write(
            Node1, 5 * ?TEST_ITEM_COUNT + 1, 6 * ?TEST_ITEM_COUNT, ?B, 2),
    [] =
        rt:systest_read(
            Node2, ?TEST_ITEM_COUNT * 2 + 1, ?TEST_ITEM_COUNT * 5, ?B, 2),
    
    lager:info("Fallbacks repaired only from node 2"),
    {RRT1A, RRFNF1A, RRPNF1A} = repair_stats(Node1),
    {RRT2A, RRFNF2A, RRPNF2A} = repair_stats(Node2),
    true = RRT1A > 0,
    true = RRFNF1A == <<"undefined">>,
    true = RRT2A > 0,
    true = RRFNF2A == RRT2A,
    true = RRPNF1A == <<"undefined">>,
    true = RRPNF2A == <<"undefined">>,

    lager:info("Repeat reads"),
    [] =
        rt:systest_read(Node1, 1, ?TEST_ITEM_COUNT * 2, ?B, 2),
    [] =
        rt:systest_read(
            Node2, ?TEST_ITEM_COUNT * 2 + 1, ?TEST_ITEM_COUNT * 5, ?B, 2),

    lager:info("Previous fallback repairs mean fewer repairs"),
    {RRT1B, RRFNF1B, RRPNF1B} = repair_stats(Node1),
    {RRT2B, RRFNF2B, RRPNF2B} = repair_stats(Node2),
    true = RRT1B > RRT1A,
    true = RRFNF1B == <<"undefined">>,
    true = RRT2B == RRT2A,
    true = RRFNF2B == RRT2B,
    true = RRPNF1B == <<"undefined">>,
    true = RRPNF2B == <<"undefined">>,

    lager:info("Restart Node3"),
    ok = rm_backend_dir(Node3),
    lager:info("Removing vnodeids!"),
    lager:info(
        "Testing with removed vnode ids resolves the issue of rogue "
        "repairs caused by key_amnesia"
    ),
    lager:info(
        "If the data backend is removed, but not the vnode ids, when the "
        "node restarts handoffs will lead to logging of key amnesia, as Node3 "
        "has forgotten changes it previously coordinated"
    ),
    lager:info(
        "With no vnode_id, a new one is generated on startup - so amnesia is "
        "not recorded.  This means that the read_repair prompted by amnesia "
        "does not happen"
    ),
    lager:info(
        "When the read repair does happen, then some will be attempted before "
        "the transfer is complete, and will not trigger read repair"
    ),
    lager:info(
        "In this case we cna see read repairs prompted on other nodes in the "
        "next systest_read"
    ),

    ok = rt:clean_data_dir([Node3], "kv_vnode"),
    rt:start_and_wait(Node3),
    ok =
        rpc:call(
            Node3, application, set_env, [riak_kv, read_repair_log, true]),
    timer:sleep(1000),
    ok = rt:wait_until_transfers_complete(Nodes),

    lager:info(
        "No primary repairs from reads to Node2 "
        "As fallback vnodes should have handed off read repairs"),
    lager:info(
        "Primary repairs now expected from Node1 though"
    ),
    ok =
        rpc:call(
            Node2, application, set_env, [riak_kv, read_repair_log, true]),
   
    [] =
        rt:systest_read(Node1, 1, ?TEST_ITEM_COUNT * 2, ?B, 2),
    [] =
        rt:systest_read(
            Node2, ?TEST_ITEM_COUNT * 2 + 1, ?TEST_ITEM_COUNT * 5, ?B, 2),
    
    {RRT1C, RRFNF1C, RRPNF1C} = repair_stats(Node1),
    {RRT2C, RRFNF2C, RRPNF2C} = repair_stats(Node2),
    lager:info("Read repair count on Node 2 ~w after reads", [RRT2C]),
    true = RRT1C > RRT1B,
    true = RRFNF1C == <<"undefined">>,
    true = RRT2C == RRT2B,

    true = RRPNF1C > 0,
    true = RRFNF2C == RRFNF2B,
    true = RRPNF2C == <<"undefined">>,

    {RCT3, CCT3} = tictacaae_accumulate_stats(Nodes),
    true = RCT3 == 0,
    true = CCT3 == 0,

    [] =
        rt:systest_read(
            Node1, 5 * ?TEST_ITEM_COUNT + 1, 6 * ?TEST_ITEM_COUNT, ?B, 2),
    {RRT1D, RRFNF1D, RRPNF1D} = repair_stats(Node1),
    true = RRT1D == RRT1C,
    true = RRFNF1D == <<"undefined">>,
    true = RRPNF1D == RRPNF1C,

    lager:info("Clear Node3 again - this time use partition repair"),
    rt:stop_and_wait(Node3),
    ok = rt:wait_until_no_pending_changes([Node1, Node2]),
    ok = rt:wait_until(fun() -> upnode_count(Node1, 3) end),
    ok = rt:wait_until(fun() -> upnode_count(Node2, 3) end),
    timer:sleep(10000),
    ok = rm_backend_dir(Node3),
    ok = rt:clean_data_dir([Node3], "kv_vnode"),
    ok = rt:clean_data_dir([Node3], "tictac_aae"),
    rt:start_and_wait(Node3),
    ok =
        rpc:call(
            Node3, application, set_env, [riak_kv, read_repair_log, true]),
    timer:sleep(1000),
    ok = rt:wait_until_transfers_complete(Nodes),
    lager:info("Set Node 3 to repair"),
    ok = 
        rpc:call(
            Node3, riak_client, repair_node, []
        ),
    timer:sleep(1000),
    ok = rt:wait_until_no_pending_changes(Nodes),
    ok = wait_until_repair_complete(Node1),
    lager:info("Re-read everything and check no read repairs"),
    [] =
        rt:systest_read(
            Node1, 1, 6 * ?TEST_ITEM_COUNT, ?B, 2),
    {RRT1E, RRFNF1E, RRPNF1E} = repair_stats(Node1),
    true = RRT1E == RRT1D,
    true = RRFNF1E == <<"undefined">>,
    true = RRPNF1E == RRPNF1D,

    pass.


check_joined(Nodes) ->
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    ?assertEqual(ok, rt:wait_until_nodes_agree_about_ownership(Nodes)).

upnode_count(Node, Count) ->
    UpNodes = rpc:call(Node, riak_core_node_watcher, nodes, [riak_kv]),
    Count == length(UpNodes).

rm_backend_dir(Node) ->
    rt:clean_data_dir([Node], backend_dir()).

backend_dir() ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    backend_dir(KVBackend).

backend_dir(undefined) ->
    backend_dir(bitcask);
backend_dir(bitcask) ->
    "bitcask";
backend_dir(eleveldb) ->
    "leveldb";
backend_dir(leveled) ->
    "leveled".

repair_stats(Node) ->
    NodeStats = verify_riak_stats:get_stats(Node, 1000),
    {<<"read_repairs_total">>, RRT} =
        lists:keyfind(<<"read_repairs_total">>, 1, NodeStats),
    {<<"read_repairs_fallback_notfound_count">>, RRFNF} =
        lists:keyfind(
            <<"read_repairs_fallback_notfound_count">>, 1, NodeStats),
    {<<"read_repairs_primary_notfound_count">>, RRPNF} =
        lists:keyfind(
            <<"read_repairs_primary_notfound_count">>, 1, NodeStats),
    lager:info(
        "For Node ~w read_repairs_total=~w"
        " fallback_notfound=~p primary_notfound=~p",
        [Node, RRT, RRFNF, RRPNF]),
    {RRT, RRFNF, RRPNF}.

tictacaae_accumulate_stats(Nodes) ->
    lists:foldl(
        fun(N, {RC, CC}) ->
            {RCN, CCN} = tictacaae_stats(N),
            {RC + RCN, CC + CCN}
        end,
        {0, 0},
        Nodes
    ).

tictacaae_stats(Node) ->
    NodeStats = verify_riak_stats:get_stats(Node, 1000),
    {<<"tictacaae_root_compare_total">>, RCT} =
        lists:keyfind(<<"tictacaae_root_compare_total">>, 1, NodeStats),
    {<<"tictacaae_clock_compare_total">>, CCT} =
        lists:keyfind(<<"tictacaae_clock_compare_total">>, 1, NodeStats),
    lager:info(
        "For Node ~w root_compare_total=~w clock_compare_total=~w",
        [Node, RCT, CCT]
    ),
    {RCT, CCT}.

-define(VERIFY_COUNT, 15).
-define(REPAIR_SLEEP, 2000).
-define(TOTAL_REPAIR_TIME, 120000).

wait_until_repair_complete(Node) ->
    wait_until_repair_complete(Node, ?TOTAL_REPAIR_TIME, false).

wait_until_repair_complete(_Node, 0, false) ->
    lager:info("Repair never started"),
    not_started;
wait_until_repair_complete(_Node, Wait, _Count) when Wait =< 0 ->
    lager:info("Repair never completed"),
    not_completed;
wait_until_repair_complete(_Node, Wait, 0) ->
    lager:info("Repair appears to have completed ~w ms remaining", [Wait]),
    ok;
wait_until_repair_complete(Node, TimeToWait, false) ->
    {TransfersPerNode, _NodesDown} =
        rpc:call(Node, riak_core_status, all_active_transfers, []),
    Transfers = lists:flatten(TransfersPerNode),
    case Transfers of
        [] ->
            timer:sleep(?REPAIR_SLEEP),
            wait_until_repair_complete(
                Node, TimeToWait - ?REPAIR_SLEEP, false);
        Transfers ->
            lager:info(
                "Transfers started ~w", [lists:flatten(Transfers)]),
            wait_until_repair_complete(Node, TimeToWait, ?VERIFY_COUNT)
    end;
wait_until_repair_complete(Node, TimeToWait, CountDown) ->
    {TransfersPerNode, _NodesDown} =
        rpc:call(Node, riak_core_status, all_active_transfers, []),
    Transfers = lists:flatten(TransfersPerNode),
    case Transfers of
        [] ->
            lager:info(
                "Repair maybe complete - validating count ~w", [CountDown]),
            timer:sleep(?REPAIR_SLEEP),
            wait_until_repair_complete(
                Node, TimeToWait - ?REPAIR_SLEEP, CountDown - 1);
        Transfers ->
            lager:info(
                "Repair ongoing ~w", [Transfers]),
            timer:sleep(?REPAIR_SLEEP),
            wait_until_repair_complete(
                Node, TimeToWait - ?REPAIR_SLEEP, ?VERIFY_COUNT)
    end.


    