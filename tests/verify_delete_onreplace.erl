%% -------------------------------------------------------------------
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
-module(verify_delete_onreplace).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_ITEM_COUNT, 20000).
-define(B, <<"systest">>).
-define(EXCHANGE_TICK, 10 * 1000). % Must be > inactivity timeout

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
        {delete_mode, DeleteMode}
    ]
    }]).

confirm() ->
    Nodes = rt:deploy_nodes(5, ?CFG(true, 3000)),
    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    InitialCluster = [Node1, Node2, Node3, Node4],
    ReplaceCluster = [Node1, Node3, Node4, Node5],
    
    lager:info("Joining initial cluster ~w", [InitialCluster]),
    lists:foreach(
        fun(N) ->
            verify_staged_clustering:stage_join(N, Node1)
        end,
        [Node2, Node3, Node4]),
    ?assertEqual(
        ok,
        rt:wait_until_all_members(InitialCluster)),
    ?assertEqual(
        ok,
        rt:wait_until_no_pending_changes(InitialCluster)),

    lager:info("Print staged plan and then commit"),
    verify_staged_clustering:print_staged(Node1),
    verify_staged_clustering:commit_staged(Node1),

    ?assertEqual(ok, rt:wait_until_nodes_ready(InitialCluster)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(InitialCluster)),
    rt:assert_nodes_agree_about_ownership(InitialCluster),

    lager:info("Join ~p to the cluster", [Node5]),
    verify_staged_clustering:stage_join(Node5, Node1),
    ?assertEqual(ok, rt:wait_until_all_members(Nodes)),

    lager:info("Stage replacement of ~p with ~p", [Node2, Node5]),
    verify_staged_clustering:stage_replace(Node1, Node2, Node5),

    lager:info("Writing ~p items", [?TEST_ITEM_COUNT]),
    [] = rt:systest_write(Node1, 1, ?TEST_ITEM_COUNT, ?B, 2),

    _ = count_tombs(Node1),

    verify_staged_clustering:print_staged(Node1),
    verify_staged_clustering:commit_staged(Node1),
    lager:info("Deleting ~p items", [?TEST_ITEM_COUNT]),
    ok = delete_batch(1000, Node1, ?TEST_ITEM_COUNT),

    _ = count_tombs(Node1),

    ?assertEqual(ok, rt:wait_until_nodes_ready(ReplaceCluster)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(ReplaceCluster)),
    rt:assert_nodes_agree_about_ownership(ReplaceCluster),

    _ = repeatedly_count_tombs(Node1, 60),

    lager:info("Resuming AAE on cluster"),
    lists:foreach(
        fun(N) ->
            rpc:call(N, riak_client, tictacaae_resume_node, [])
        end,
        ReplaceCluster
    ),
    timer:sleep(?EXCHANGE_TICK),

    _ = repeatedly_count_tombs(Node1, 60),

    pass.


repeatedly_count_tombs(Node, 0) ->
    count_tombs(Node);
repeatedly_count_tombs(Node, N) ->
    count_tombs(Node),
    timer:sleep(2000),
    repeatedly_count_tombs(Node, N - 1).


count_tombs(Node) ->
    {ok, TC0} =
        rpc:call(
            Node,
            riak_client,
            aae_fold,
            [{reap_tombs, ?B, all, all, all, count}]),
    lager:info("Tombstone count ~w", [TC0]),
    TC0.

delete_batch(BatchSize, Node, Total) ->
    delete_batch(Node, 1, BatchSize, Total).

delete_batch(_Node, NextBatch, _BatchSize, Total) when NextBatch >= Total ->
    ok;
delete_batch(Node, NextBatch, BatchSize, Total) ->
    lager:info("Deleting ~p items", [BatchSize]),
    [] = rt:systest_delete(Node, NextBatch, NextBatch + BatchSize - 1, ?B, 2),
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    lager:info("Owners now ~p", [riak_core_ring:all_owners(Ring)]),
    delete_batch(Node, NextBatch + BatchSize, BatchSize, Total).