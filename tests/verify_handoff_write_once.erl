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
-module(verify_handoff_write_once).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-define(BUCKET_TYPE, <<"write_once">>).
-define(BUCKET, {?BUCKET_TYPE, <<"write_once">>}).


%% @doc
confirm() ->

    AsyncConfig  = create_config(riak_kv_eleveldb_backend),
    AsyncCluster = run_test(AsyncConfig, true),

    rt:clean_cluster(AsyncCluster),

    SyncConfig   = create_config(riak_kv_memory_backend),
    _SyncCluster = run_test(SyncConfig, false),

    pass.

create_config(Backend) ->
    [{riak_core, [
        {default_bucket_props, [{n_val, 1}]},
        {ring_creation_size, 8},
        {handoff_acksync_threshold, 20},
        {handoff_concurrency, 4},
        {handoff_receive_timeout, 2000},
        {vnode_management_timer, 100}]},
    {riak_kv, [
        {storage_backend, Backend}]}
    ].

run_test(Config, AsyncWrites) ->
    %%
    %% Deploy 2 nodes based on config.  Wait for K/V to start on each node.
    %%
    lager:info("Deploying 2 nodes..."),
    Cluster = [RootNode, NewNode] = deploy_test_nodes(2, Config),
    [rt:wait_for_service(Node, riak_kv) || Node <- [RootNode, NewNode]],
    %%
    %% Set up the intercepts
    %%
    lager:info("Setting up intercepts..."),
    make_intercepts_tab(RootNode),
    % This intercept will tell the backround process (below) to send an event for each
    % vnode that is being handed off (there will be 4 such vnodes, in this test case)
    rt_intercept:add(
        RootNode, {riak_kv_worker, [{{handle_work, 3}, handle_work_intercept}]}
    ),
    rt_intercept:add(
        RootNode, {riak_kv_vnode, [
            %% Count everytime riak_kv_vnode:handle_handoff_command/3 is called with a write_once message
            {{handle_handoff_command, 3}, count_handoff_w1c_puts},
            %% Count everytime riak_kv_vnode:handle_handoff_command/3 is called with a write_once message
            {{handle_command, 3}, count_w1c_handle_command}
        ]}
    ),
    true = rpc:call(RootNode, ets, insert, [intercepts_tab, {w1c_async_replies, 0}]),
    true = rpc:call(RootNode, ets, insert, [intercepts_tab, {w1c_sync_replies, 0}]),
    true = rpc:call(RootNode, ets, insert, [intercepts_tab, {w1c_put_counter, 0}]),
    %%
    %% Seed the root node with some data
    %%
    lager:info("Populating root node..."),
    rt:create_and_activate_bucket_type(RootNode, ?BUCKET_TYPE, [{write_once, true}, {n_val, 1}]),
    NTestItems = 1000,
    RingSize = proplists:get_value(ring_creation_size, proplists:get_value(riak_core, Config)),
    rt:systest_write(RootNode, 1, NTestItems, ?BUCKET, 1),
    %%
    %% Start an asynchronous proc which will send puts into riak during handoff.
    %%
    lager:info("Joining new node with cluster..."),
    start_proc(RootNode, NTestItems),
    timer:sleep(1000),
    rt:join(NewNode, RootNode),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Cluster)),
    rt:wait_until_no_pending_changes(Cluster),
    rt:wait_until_transfers_complete(Cluster),
    TotalSent = stop_proc(),
    %%
    %% Verify the results
    %%
    lager:info("Validating data after handoff..."),
    Results2 = rt:systest_read(NewNode, 1, TotalSent, ?BUCKET, 1),
    ?assertEqual([], Results2),
    lager:info("Read ~p entries.", [TotalSent]),
    [{_, Count}] = rpc:call(RootNode, ets, lookup, [intercepts_tab, w1c_put_counter]),
    ?assertEqual(RingSize div 2, Count),
    lager:info("We handled ~p write_once puts during handoff.", [Count]),
    [{_, W1CAsyncReplies}] = rpc:call(RootNode, ets, lookup, [intercepts_tab, w1c_async_replies]),
    [{_, W1CSyncReplies}]  = rpc:call(RootNode, ets, lookup, [intercepts_tab, w1c_sync_replies]),
    %% NB. We should expect RingSize replies, because 4 handoffs should run in both directions
    case AsyncWrites of
        true ->
            ?assertEqual(NTestItems + RingSize, W1CAsyncReplies),
            ?assertEqual(0, W1CSyncReplies);
        false ->
            ?assertEqual(0, W1CAsyncReplies),
            ?assertEqual(NTestItems + RingSize, W1CSyncReplies)
    end,
    %%
    Cluster.

deploy_test_nodes(N, Config) ->
    rt:deploy_nodes(N, Config).

make_intercepts_tab(Node) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    intercepts_tab = rpc:call(Node, ets, new, [intercepts_tab, [named_table,
                public, set, {heir, SupPid, {}}]]).


-record(state, {
    node, sender, k
}).

start_proc(Node, NTestItems) ->
    Self = self(),
    Pid = spawn_link(fun() -> loop(#state{node=Node, sender=Self, k=NTestItems}) end),
    global:register_name(start_fold_started_proc, Pid).

loop(#state{node=Node, sender=Sender, k=K} = State) ->
    receive
        stop ->
            Sender ! K;
        {write, Pid} ->
            rt:systest_write(Node, K, K + 1, ?BUCKET, 1),
            lager:info("Asynchronously wrote event ~p during handoff.", [K + 1]),
            Pid ! ok,
            loop(State#state{k=K + 1});
        Msg ->
            lager:warning("~p: Unexpected Message: ~p.  Ignoring...", [?MODULE, Msg]),
            loop(State)
    end.

stop_proc() ->
    catch global:send(start_fold_started_proc, stop),
    receive
        K -> K
    end.
