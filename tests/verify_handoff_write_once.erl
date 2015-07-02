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
-define(RING_SIZE, 8).
-define(NVAL, 1).
-define(CONFIG,
    [
        {riak_core, [
            {default_bucket_props, [{n_val, ?NVAL}]},
            {vnode_management_timer, 1000},
            {ring_creation_size, ?RING_SIZE}]
        },
        {riak_kv, [
            {anti_entropy_build_limit, {100, 1000}},
            {anti_entropy_concurrency, 100},
            {anti_entropy_tick, 100},
            {anti_entropy, {on, []}},
            {anti_entropy_timeout, 5000}
        ]}
    ]
).

-record(state, {
    sender, node, k, total = 0
}).

%%
%% @doc This test is going to set up a 2-node cluster, with a write-once bucket with an nval
%% of 1.  We also set up an intercept on the pref-list generation to ensure that all
%% puts will go to exactly one of the nodes in the cluster.  (Note: there is some sensitivity
%% in the intercept to the node name, which is not something we parameterize to the intercept.
%% If something fundamental changes in the generation of node names (e.g., via make stagedevrel
%% in riak/_ee, then this test may break).
%% Once the cluster is set up, we write NTestItems to the cluster.
%%
confirm() ->
    NTestItems    = 1000,
    NTestNodes    = 2,

    run_test(NTestItems, NTestNodes),

    lager:info("Test verify_handoff_write_once passed."),
    pass.


run_test(NTestItems, NTestNodes) ->
    lager:info("Testing handoff (items ~p, encoding: default)", [NTestItems]),
    %%
    %% Build a 2-node cluster
    %%
    Cluster = rt:build_cluster(NTestNodes, ?CONFIG),
    lager:info("Set up cluster: ~p", [Cluster]),
    %%
    %% Use the first node to create an immutable bucket (type)
    %%
    [Node1, Node2] = Cluster,
    rt:wait_for_service(Node1, riak_kv),
    rt:create_and_activate_bucket_type(Node1, ?BUCKET_TYPE, [{write_once, true}, {n_val, 1}]),
    rt:wait_until_bucket_type_status(?BUCKET_TYPE, active, Cluster),
    %%
    %% Add intercepts:
    %%    - slow down the handoff mechanism to trigger handloffs while removing nodes
    %%    - count of the number of handoffs, and
    %%    - force the preflist to send all puts to partition 0.
    %%
    make_intercepts_tab(Node1),
    %% Insert delay into handoff folding
    rt_intercept:add(Node1, {riak_core_handoff_sender, [{{visit_item, 3}, slightly_delayed_visit_item_3}]}),

    rt_intercept:add(Node1, {riak_kv_vnode, [{{handle_handoff_command, 3}, count_handoff_w1c_puts}]}),
    rt_intercept:add(Node1, {riak_core_apl, [{{get_apl_ann, 3}, force_dev1}]}),
    rt_intercept:add(Node2, {riak_core_apl, [{{get_apl_ann, 3}, force_dev2}]}),
    %%
    %% Write NTestItems entries to a write_once bucket
    %%
    [] = rt:systest_write(Node1, 1, NTestItems, ?BUCKET, 1),
    lager:info("Wrote ~p entries.", [NTestItems]),
    %%
    %% Evict Node1, and wait until handoff completes
    %% Run some puts asynchronously to trigger handoff command
    %% in the vnode.
    %%
    lager:info("Evicting Node1..."),
    Pid = start_proc(Node1, NTestItems),
    rt:leave(Node1),
    timer:sleep(1001), % let the proc send some data
    rt:wait_until_transfers_complete(Cluster),
    lager:info("Transfers complete."),
    %%
    %%
    %%
    TotalSent = stop_proc(Pid),
    lager:info("TotalSent: ~p", [TotalSent]),
    [{_, Handoffs}] = rpc:call(Node1, ets, lookup, [intercepts_tab, w1c_put_counter]),
    ?assert(Handoffs > 0),
    lager:info("Looking Good. We handled ~p handoffs.", [Handoffs]),
    %%
    %% verify NumTestItems hits through handoff, and that NumTestItems are in the remaining cluster
    %%
    %rt:stop(Node1),
    Results = rt:systest_read(Node2, 1, TotalSent, ?BUCKET, 1),
    ?assertEqual(0, length(Results)),
    lager:info("Data looks ok; we can retrieve ~p entries from the cluster.", [TotalSent]),
    ok.


make_intercepts_tab(Node) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    intercepts_tab = rpc:call(
        Node, ets, new,
        [intercepts_tab, [named_table, public, set, {heir, SupPid, {}}]]
    ),
    rpc:call(Node, ets, insert, [intercepts_tab, {w1c_put_counter, 0}]).



start_proc(Node, NTestItems) ->
    Self = self(),
    spawn_link(fun() -> loop(#state{sender=Self, node=Node, k=NTestItems}) end).


loop(#state{sender=Sender, node=Node, k=K, total=Total} = State) ->
    {Done, NewState} = receive
        done ->
            {true, Sender ! K}
    after 2 ->
        {
            false,
            if Total < 500 ->
                case rt:systest_write(Node, K, K+1, ?BUCKET, 1) of
                    [] -> State#state{k=K+1, total=Total+1};
                    _ ->  State
                end;
            true ->
                State
            end
        }
    end,
    case Done of
        true ->  lager:info("Asynchronously added ~p entries.", [Total]), ok;
        false -> loop(NewState)
    end.


stop_proc(Pid) ->
    Pid ! done,
    receive
        K -> K
    end.