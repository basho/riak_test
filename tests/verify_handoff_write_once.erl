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


%% @doc This test will run a handoff in the case of write_once buckets, verifying
%% that write-once entries are properly handed off as part of ownership handoff,
%% but more importantly, that riak_kv_vnode properly handles data being written into
%% riak while ownership handoff is taking place.
%%
%% This test will create two nodes each with a ring size of 8, and populate one node
%% with 1k entries.  It will then join the two nodes to make a cluster of size 2, which
%% will result in ownership handoff of four of the nodes (in each direction).
%%
%% We have intercepted the riak_kv_worker, which handles handoff for an individual vnode,
%% to ensure what we can send data through Riak while the cluster is in the handoff state,
%% thus ensuring that the riak_kv_vnode:handle_handoff_command callback is exercised in
%% the case of write_once buckets.
%%
%% We install intercepts at key points in the vnode to measure how many time various key
%% parts of the code are called.
%%
%% We run the above test twice, once in the case where we are doing asynchronous writes on the
%% back end, and once when we are using synchronous writes.  Currently, this is toggled via
%% the use of a back end that can support async writes (currently, only leveldb)
%%
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
    Cluster = [RootNode, NewNode] = rt:deploy_nodes(2, Config),
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
            %% Count everytime riak_kv_vnode:handle_command/3 is called with a write_once message
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
    NTestItems = 100,
    RingSize = proplists:get_value(ring_creation_size, proplists:get_value(riak_core, Config)),
    [] = rt:systest_write(RootNode, 1, NTestItems, ?BUCKET, 1),
    %%
    %% Start an asynchronous proc which will send puts into riak during handoff.
    %%
    lager:info("Joining new node with cluster..."),
    start_proc(RootNode, NTestItems, RingSize div 2),
    rt:join(NewNode, RootNode),
    TotalSent = wait_until_async_writes_complete(),
    ?assertMatch(ok, rt:wait_until_nodes_ready(Cluster)),
    rt:wait_until_bucket_type_visible(Cluster, ?BUCKET_TYPE),
    rt:wait_until_no_pending_changes(Cluster),
    rt:wait_until_transfers_complete(Cluster),
    %%
    %% Verify the results
    %%
    lager:info("Validating data after handoff..."),
    Results2 = rt:systest_read(NewNode, 1, TotalSent, ?BUCKET, 1),
    ?assertMatch([], Results2),
    lager:info("Read ~p entries.", [TotalSent]),
    [{_, Count}] = rpc:call(RootNode, ets, lookup, [intercepts_tab, w1c_put_counter]),
    ?assertEqual(RingSize div 2, Count),
    lager:info("We handled ~p write_once puts during handoff.", [Count]),
    [{_, W1CAsyncReplies}] = rpc:call(RootNode, ets, lookup, [intercepts_tab, w1c_async_replies]),
    [{_, W1CSyncReplies}]  = rpc:call(RootNode, ets, lookup, [intercepts_tab, w1c_sync_replies]),
    case AsyncWrites of
        true ->
            ?assertEqual(NTestItems + RingSize div 2, W1CAsyncReplies),
            ?assertEqual(0, W1CSyncReplies);
        false ->
            ?assertEqual(0, W1CAsyncReplies),
            ?assertEqual(NTestItems + RingSize div 2, W1CSyncReplies)
    end,
    Cluster.

make_intercepts_tab(Node) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    intercepts_tab = rpc:call(Node, ets, new, [intercepts_tab, [named_table,
                public, set, {heir, SupPid, {}}]]).


%%
%% Notes on the background process and corresponding intercepts.
%%
%% The code below is used to spawn a background process that is globally
%% registered with the name rt_ho_w1c_proc.  This process will
%% wait for a message from the riak_kv_worker handle_work intercept,
%% telling this proc to write a message into Riak.  The timing of the
%% intercept is such that the write is guaranteed to take place while
%% handoff is in progress, but before the vnode has been told to finish.
%% Sending this message will trigger this background process to do a
%% write into Riak, which in turn will force the vnode's
%% handle_handoff_command to be called.
%%

-record(state, {
    node, sender, k, pids=[], expected, init=true
}).

start_proc(Node, NTestItems, Expected) ->
    Self = self(),
    Pid = spawn_link(fun() -> loop(#state{node=Node, sender=Self, k=NTestItems, expected=Expected}) end),
    global:register_name(rt_ho_w1c_proc, Pid),
    receive ok -> ok end.

loop(#state{node=Node, sender=Sender, k=K, pids=Pids, expected=Expected, init=Init} = State) ->
    case Init of
        true ->
            Sender ! ok;
        _ -> ok
    end,
    receive
        {write, Pid} ->
            ThePids = [Pid | Pids],
            NumPids = length(ThePids),
            case NumPids of
                Expected ->
                    %%
                    %% The number of expected vnodes are now in the handoff state.  Do some writes, and send ok's
                    %% back to the waiting vnodes.  Once they get the ok back, they will complete handoff.  At this
                    %% point, we are done, so we can tell the test to proceed and wait for handoff to complete.
                    %%
                    [] = rt:systest_write(Node, K + 1, K + Expected, ?BUCKET, 1),
                    lager:info(
                        "Asynchronously wrote entries [~p..~p] during handoff.  Sending ok's back to ~p waiting vnode(s)...",
                        [K + 1, K + Expected, NumPids]
                    ),
                    [ThePid ! ok || ThePid <- ThePids],
                    Sender ! (K + Expected);
                _ ->
                    loop(State#state{pids=ThePids, init=false})
            end
    end.


wait_until_async_writes_complete() ->
    receive
        K -> K
    after 60000 ->
        throw("Timed out after 60s waiting for async writes to complete.")
    end.