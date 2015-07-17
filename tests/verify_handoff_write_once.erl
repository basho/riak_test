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
-module(verify_handoff_write_once).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-define(BUCKET_TYPE, <<"write_once">>).
-define(BUCKET, {?BUCKET_TYPE, <<"write_once">>}).

%% We've got a separate test for capability negotiation and other mechanisms, so the test here is fairly
%% straightforward: get a list of different versions of nodes and join them into a cluster, making sure that
%% each time our data has been replicated:
confirm() ->
    NTestItems    = 1000,                                   %% How many test items to write/verify?
    NTestNodes    = 2,                                      %% How many nodes to spin up for tests?

    run_test(NTestItems, NTestNodes),

    lager:info("Test verify_handoff passed."),
    pass.

run_test(NTestItems, NTestNodes) ->
    lager:info("Testing handoff (items ~p, encoding: default)", [NTestItems]),

    lager:info("Spinning up test nodes"),
    [RootNode | TestNodes] = deploy_test_nodes(NTestNodes),

    rt:wait_for_service(RootNode, riak_kv),

    make_intercepts_tab(RootNode),

    % This intercept will tell the backround process (below) to send an event for each
    % vnode that is being handed off (there will be 4 such vnodes, in this test case)
    rt_intercept:add(
        RootNode, {riak_kv_worker, [{{handle_work, 3}, handle_work_intercept}]}
    ),
    %% Count everytime riak_kv_vnode:handle_handoff_command/3 is called with a write_once message
    rt_intercept:add(
        RootNode, {riak_kv_vnode, [{{handle_handoff_command, 3}, count_handoff_w1c_puts}]}
    ),

    lager:info("Populating root node."),
    rt:create_and_activate_bucket_type(RootNode, ?BUCKET_TYPE, [{write_once, true}]),
    rt:systest_write(RootNode, 1, NTestItems, ?BUCKET, 1),

    lager:info("Testing handoff for cluster."),
    lists:foreach(fun(TestNode) -> test_handoff(RootNode, TestNode, NTestItems) end, TestNodes).

%% See if we get the same data back from our new nodes as we put into the root node:
test_handoff(RootNode, NewNode, NTestItems) ->

    lager:info("Waiting for service on new node."),
    rt:wait_for_service(NewNode, riak_kv),

    %% Set the w1c_put counter to 0
    true = rpc:call(RootNode, ets, insert, [intercepts_tab, {w1c_put_counter, 0}]),

    lager:info("Joining new node with cluster."),
    start_proc(RootNode, NTestItems),
    timer:sleep(1000),
    rt:join(NewNode, RootNode),
    ?assertEqual(ok, rt:wait_until_nodes_ready([RootNode, NewNode])),
    rt:wait_until_no_pending_changes([RootNode, NewNode]),
    TotalSent = stop_proc(),

    %% See if we get the same data back from the joined node that we added to the root node.
    %%  Note: systest_read() returns /non-matching/ items, so getting nothing back is good:
    lager:info("Validating data after handoff..."),
    Results2 = rt:systest_read(NewNode, 1, TotalSent, ?BUCKET, 1),
    ?assertEqual([], Results2),
    lager:info("Data looks good.  Read ~p entries.", [TotalSent]),
    [{_, Count}] = rpc:call(RootNode, ets, lookup, [intercepts_tab, w1c_put_counter]),
    ?assert(Count > 0),
    lager:info("Looking Good. We handled ~p write_once puts during handoff.", [Count]).

deploy_test_nodes(N) ->
    Config = [{riak_core, [{default_bucket_props, [{n_val, 1}]},
                           {ring_creation_size, 8},
                           {handoff_acksync_threshold, 20},
                           {handoff_concurrency, 4},
                           {handoff_receive_timeout, 2000},
                           {vnode_management_timer, 1000}]}],
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
