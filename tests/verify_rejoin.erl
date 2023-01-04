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

-define(TEST_ITEM_COUNT, 10000).
-define(B, <<"systest">>).

-define(CFG, 
            [{riak_core,
                [{ring_creation_size, 16},
                {vnode_inactivity_timeout, 5 * 1000}]},
            {riak_kv, 
                [{anti_entropy, {off, []}},
                {tictacaae_active, active},
                {tictacaae_parallelstore, leveled_ko},
                        % if backend not leveled will use parallel key-ordered
                        % store
                {tictacaae_exchangetick, 10 * 1000}, % 10 seconds, > inactivity timeout
                {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
                {tictacaae_primaryonly, true}]
            }]).

confirm() ->
    %% Bring up a 3-node cluster for the test

    lager:info("Leave and join nodes during write activity"),
    lager:info("Do this with aggressive AAE - but we shouldn't see AAE repairs"),
    lager:info("In the future - need a riak stat for repairs to validate this"),
    Nodes = rt:build_cluster(4, ?CFG),
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
    lager:info("Will check the tictac_deltacount on state"),
    lager:info("This may be brittle to changes in riak_kv_vnode state"),
    lager:info("TODO - Add riak stat"),
    Ring = rt:get_ring(Node1),
    Owners = riak_core_ring:all_owners(Ring),
    timer:sleep(10000),
    lists:foreach(fun check_vnode_stats/1, Owners),
    timer:sleep(10000),
    lists:foreach(fun check_vnode_stats/1, Owners),
    timer:sleep(10000),
    lists:foreach(fun check_vnode_stats/1, Owners),

    lager:info("Check all values read"),
    [] = rt:systest_read(Node1, 5 * ?TEST_ITEM_COUNT),
    
    {0, <<"undefined">>} = repair_stats(Node1),
    {0, <<"undefined">>} = repair_stats(Node2),

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
    {RRT1, RRFNF1} = repair_stats(Node1),
    {RRT2, RRFNF2} = repair_stats(Node2),
    true = RRT1 > 0,
    true = RRFNF1 == <<"undefined">>,
    true = RRT2 > 0,
    true = RRFNF2 == RRT2,

    lager:info("Repeat reads"),
    [] =
        rt:systest_read(Node1, 1, ?TEST_ITEM_COUNT * 2, ?B, 2),
    [] =
        rt:systest_read(
            Node2, ?TEST_ITEM_COUNT * 2 + 1, ?TEST_ITEM_COUNT * 5, ?B, 2),

    lager:info("Previous fallback repairs mean fewer repairs"),
    {RRT1B, RRFNF1B} = repair_stats(Node1),
    {RRT2B, RRFNF2B} = repair_stats(Node2),
    true = RRT1B > RRT1,
    true = RRFNF1 == <<"undefined">>,
    true = RRT2B == RRT2,

    lager:info("Restart Node3"),
    rt:start_and_wait(Node3),
    ok = rt:wait_until_node_handoffs_complete(Node1),
    ok = rt:wait_until_node_handoffs_complete(Node2),

    [] = rt:systest_read(Node1, 6 * ?TEST_ITEM_COUNT),

    pass.


check_joined(Nodes) ->
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    ?assertEqual(ok, rt:wait_until_nodes_agree_about_ownership(Nodes)).

check_vnode_stats({Partition, Node}) ->
    {ok, Pid} =
        rpc:call(Node,
                    riak_core_vnode_manager,
                    get_vnode_pid,
                    [Partition, riak_kv_vnode]),
    {riak_kv_vnode, ModState} =
        rpc:call(Node,
                    riak_core_vnode,
                    get_modstate,
                    [Pid]),
    ?assertEqual(0, element(27, ModState)).

upnode_count(Node, Count) ->
    UpNodes = rpc:call(Node, riak_core_node_watcher, nodes, [riak_kv]),
    Count == length(UpNodes).

repair_stats(Node) ->
    NodeStats = verify_riak_stats:get_stats(Node, 1000),
    {<<"read_repairs_total">>, RRT} =
        lists:keyfind(<<"read_repairs_total">>, 1, NodeStats),
    {<<"read_repairs">>, RR} =
        lists:keyfind(<<"read_repairs">>, 1, NodeStats),
    {<<"read_repairs_fallback_notfound_count">>, RRFNF} =
        lists:keyfind(
            <<"read_repairs_fallback_notfound_count">>, 1, NodeStats),
    lager:info(
        "For Node ~w read_repairs=~w read_repairs_total=~w"
        " fallback_notfound=~w",
        [Node, RR, RRT, RRFNF]),
    {RRT, RRFNF}.