%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

-module(ensemble_sync).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NVal = 5,
    Config = ensemble_util:fast_config(NVal),
    Nodes = ensemble_util:build_cluster(8, Config, NVal),
    Node = hd(Nodes),
    vnode_util:load(Nodes),

    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal),

    ExpectOkay    = [ok],
    ExpectTimeout = [{error, timeout}, {error, <<"timeout">>},
                     {error, <<"failed">>} | ExpectOkay],
    ExpectFail    = [{error, notfound} | ExpectTimeout],

    Scenarios = [%% corrupted, suspended, valid, empty, bucket, expect
                 {1, 1, 1, 2, <<"test1">>, ExpectOkay},
                 {1, 2, 0, 2, <<"test2">>, ExpectTimeout},
                 {2, 1, 0, 2, <<"test3">>, ExpectTimeout},
                 {3, 0, 0, 2, <<"test4">>, ExpectFail}
                ],
    [ok = run_scenario(Nodes, NVal, Scenario) || Scenario <- Scenarios],
    pass.

-spec partition(non_neg_integer(), node(), list()) -> {[{non_neg_integer(), node()}], [node()]}.
partition(Minority, ContactNode, PL) ->
    AllVnodes = [VN || {VN, _} <- PL],
    OtherVnodes = [VN || {VN={_, Owner}, _} <- PL,
                   Owner =/= ContactNode],
    NodeCounts = num_partitions_per_node(OtherVnodes),
    PartitionedNodes = minority_nodes(NodeCounts, Minority),
    PartitionedVnodes = minority_vnodes(OtherVnodes, PartitionedNodes),
    ValidVnodes = AllVnodes -- PartitionedVnodes,
    {ValidVnodes, PartitionedNodes}.

num_partitions_per_node(Other) ->
    lists:foldl(fun({_, Node}, Acc) ->
                    orddict:update_counter(Node, 1, Acc)
                end, orddict:new(), Other).

minority_nodes(NodeCounts, MinoritySize) ->
    lists:foldl(fun({Node, Count}, Acc) ->
                    case Count =:= 1 andalso length(Acc) < MinoritySize of
                        true ->
                            [Node | Acc];
                        false ->
                            Acc
                    end
                end, [], NodeCounts).

minority_vnodes(Vnodes, PartitionedNodes) ->
    [VN || {_, Node}=VN <- Vnodes, lists:member(Node, PartitionedNodes)].

run_scenario(Nodes, NVal, {NumKill, NumSuspend, NumValid, _, Name, Expect}) ->
    Node = hd(Nodes),
    Quorum = NVal div 2 + 1,
    Minority = NVal - Quorum,
    Bucket = {<<"strong">>, Name},
    Keys = [<<N:64/integer>> || N <- lists:seq(1,1000)],

    Key1 = hd(Keys),
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key1}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    {Valid, Partitioned} = partition(Minority, Node, PL),

    {KillVN,    Valid2} = lists:split(NumKill,    Valid),
    {SuspendVN, Valid3} = lists:split(NumSuspend, Valid2),
    {AfterVN,   _}      = lists:split(NumValid,   Valid3),

    io:format("PL: ~p~n", [PL]),
    PBC = rt_pb:pbc(Node),
    Options = [{timeout, 2000}],

    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [manual]),
    Part = rt_node:partition(Nodes -- Partitioned, Partitioned),
    ensemble_util:wait_until_stable(Node, Quorum),

    %% Write data while minority is partitioned
    lager:info("Writing ~p consistent keys", [1000]),
    [ok = rt_pb:pbc_write(PBC, Bucket, Key, Key) || Key <- Keys],

    lager:info("Read keys to verify they exist"),
    [rt_pb:pbc_read(PBC, Bucket, Key, Options) || Key <- Keys],
    rt_node:heal(Part),

    %% Suspend desired number of valid vnodes
    S1 = [vnode_util:suspend_vnode(VNode, VIdx) || {VIdx, VNode} <- SuspendVN],

    %% Kill/corrupt desired number of valid vnodes
    [vnode_util:kill_vnode(VN) || VN <- KillVN],
    [vnode_util:rebuild_vnode(VN) || VN <- KillVN],
    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [automatic]),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Disabling AAE"),
    rpc:multicall(Nodes, riak_kv_entropy_manager, disable, []),
    ensemble_util:wait_until_stable(Node, Quorum),

    %% Suspend remaining valid vnodes to ensure data comes from repaired vnodes
    S2 = [vnode_util:suspend_vnode(VNode, VIdx) || {VIdx, VNode} <- AfterVN],
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Checking that key results match scenario"),
    [rt_pb:pbc_read_check(PBC, Bucket, Key, Expect, Options) || Key <- Keys],

    lager:info("Re-enabling AAE"),
    rpc:multicall(Nodes, riak_kv_entropy_manager, enable, []),

    lager:info("Resuming all vnodes"),
    [vnode_util:resume_vnode(Pid) || Pid <- S1 ++ S2],
    ensemble_util:wait_until_stable(Node, NVal),

    %% Check that for other than the "all bets are off" failure case,
    %% we can successfully read all keys after all vnodes are available.
    case lists:member({error, notfound}, Expect) of
        true ->
            ok;
        false ->
            lager:info("Re-reading keys to verify they exist"),
            [rt_pb:pbc_read(PBC, Bucket, Key, Options) || Key <- Keys]
    end,

    lager:info("Scenario passed"),
    lager:info("-----------------------------------------------------"),
    ok.
