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

-define(INTERCEPT_TAB, intercept_leader_tick_counts).

confirm() ->
    NVal = 5,
    Config = ensemble_util:fast_config(NVal),
    Nodes = ensemble_util:build_cluster(8, Config, NVal),
    lists:foreach(fun init_intercepts/1, Nodes),
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
    lager:info("Scenario: ~p ~p ~p ~p ~p",
                [NumKill, NumSuspend, NumValid, Name, Expect]),
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
    {AfterVN,   Valid4}      = lists:split(NumValid,   Valid3),
    
    lager:info(
        "PL: ~p " ++
        "Post-Partition: ~p Post-Kill: ~p Post-Suspend: ~p Post-Suspend2: ~p",
            [PL, Valid, Valid2, Valid3, Valid4]),

    PBC = rt:pbc(Node),
    Options = [{timeout, 2000}],

    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [manual]),
    Part = rt:partition(Nodes -- Partitioned, Partitioned),
    wait_for_leader_tick_changes(Nodes),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Writing ~p consistent keys whilst partitioned", [1000]),
    WriteFun =
        fun(Key) ->
            ok =
                case rt:pbc_write(PBC, Bucket, Key, Key, "text/plain", Options) of
                    ok ->
                        ok;
                    E ->
                        lager:info("Error ~w with Key ~p", [E, Key]),
                        E
                end
        end,
    lists:foreach(WriteFun, Keys),

    lager:info("Read keys to verify they exist"),
    [rt:pbc_read(PBC, Bucket, Key, Options) || Key <- Keys],

    lager:info("Heal partition"),
    rt:heal(Part),
    lager:info("Read keys to confirm they exist after heal"),
    [rt:pbc_read(PBC, Bucket, Key, Options) || Key <- Keys],

    %% Suspend desired number of valid vnodes
    lager:info("Suspend vnodes ~p", [SuspendVN]),
    S1 = [vnode_util:suspend_vnode(VNode, VIdx) || {VIdx, VNode} <- SuspendVN],

    %% Kill/corrupt desired number of valid vnodes
    lager:info("Kill vnodes ~p", [KillVN]),
    [vnode_util:kill_vnode(VN) || VN <- KillVN],
    lager:info("Rebuild AAE trees on vnodes ~p", [KillVN]),
    [vnode_util:rebuild_vnode(VN) || VN <- KillVN],
    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [automatic]),
    wait_for_leader_tick_changes(Nodes),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Sleep a minute to allow time for AAE"),
    timer:sleep(60000),
    
    lager:info("Disabling AAE"),
    rpc:multicall(Nodes, riak_kv_entropy_manager, disable, []),
    ensemble_util:wait_until_stable(Node, Quorum),

    %% Suspend remaining valid vnodes to ensure data comes from repaired vnodes
    S2 = [vnode_util:suspend_vnode(VNode, VIdx) || {VIdx, VNode} <- AfterVN],
    wait_for_leader_tick_changes(Nodes),
    ensemble_util:wait_until_stable(Node, Quorum),

    lager:info("Checking that key results match scenario - without AAE"),
    [rt:pbc_read_check(PBC, Bucket, Key, Expect, Options) || Key <- Keys],

    lager:info("Re-enabling AAE"),
    rpc:multicall(Nodes, riak_kv_entropy_manager, enable, []),

    lager:info("Resuming all vnodes"),
    [vnode_util:resume_vnode(Pid) || Pid <- S1 ++ S2],
    wait_for_leader_tick_changes(Nodes),
    ensemble_util:wait_until_stable(Node, NVal),

    %% Check that for other than the "all bets are off" failure case,
    %% we can successfully read all keys after all vnodes are available.
    case lists:member({error, notfound}, Expect) of
        true ->
            ok;
        false ->
            lager:info("Re-reading keys to verify they exist"),
            [rt:pbc_read(PBC, Bucket, Key, Options) || Key <- Keys]
    end,

    lager:info("Scenario passed"),
    lager:info("-----------------------------------------------------"),
    ok.

%% The following code is used so that we can wait for ensemble leader ticks to fire.
%% This allows us to fix a kind of race condition that we were dealing with in the
%% previous version of this test, where we were relying on ensemble_util:wait_until_stable
%% after making certain changes to the cluster.
init_intercepts(Node) ->
    make_intercepts_tab(Node),
    rt_intercept:add(Node, {riak_ensemble_peer, [{{leader_tick, 1}, count_leader_ticks}]}).

make_intercepts_tab(Node) ->
    SupPid = rpc:call(Node, erlang, whereis, [sasl_safe_sup]),
    Opts = [named_table, public, set, {heir, SupPid, {}}],
    ?INTERCEPT_TAB = rpc:call(Node, ets, new, [?INTERCEPT_TAB, Opts]).

get_leader_tick_counts(Nodes) ->
    AllCounts = [get_leader_tick_counts_for_node(N) || N <- Nodes],
    lists:append(AllCounts).

get_leader_tick_counts_for_node(Node) ->
    Ensembles = rpc:call(Node, riak_kv_ensembles, local_ensembles, []),
    Leaders = rpc:call(Node, lists, map, [fun riak_ensemble_manager:get_leader_pid/1, Ensembles]),
    LocalLeaders = [P || P <- Leaders, node(P) =:= Node],
    LookupFun = fun(P) ->
                        [Res] = rpc:call(Node, ets, lookup, [?INTERCEPT_TAB, P]),
                        Res
                end,
    lists:map(LookupFun, LocalLeaders).

wait_for_leader_tick_changes(Nodes) ->
    Counts = get_leader_tick_counts(Nodes),
    lists:foreach(fun wait_for_leader_tick_change/1, Counts).

wait_for_leader_tick_change({Pid, Count}) ->
    F = fun() -> leader_tick_count_exceeds(Pid, Count) end,
    ?assertEqual(ok, rt:wait_until(F)).

leader_tick_count_exceeds(Pid, Count) ->
    Node = node(Pid),
    case rpc:call(Node, ets, lookup, [?INTERCEPT_TAB, Pid]) of
        [{Pid, NewCount}] when NewCount > Count ->
            true;
        Res ->
            %% If the count hasn't incremented, it may be because the leader
            %% already stepped down, so check for that scenario as well:
            case rpc:call(Node, sys, get_state, [Pid]) of
                {leading, _} ->
                    Res;
                Res2 = {badrpc, _} ->
                    {Res, Res2};
                {_, _} ->
                    %% Would be nice if there was a more explicit way to match
                    %% this, but if it's not a badrpc and we're not leading, we
                    %% must be in some other state
                    true
            end
    end.
