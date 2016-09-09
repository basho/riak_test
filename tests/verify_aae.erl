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
%% @doc Verification of Active Anti Entropy.
%% The basic guarantee of AAE is this: Even without the read repairs that will
%% happen when data is accessed, inconsistencies between the replicas of a
%% KV object will be repaired eventually.  The test tries hard not to
%% explicitly check for when the AAE trees are built or when exchanges are run
%% in an effort to remain decoupled from the implementation.  Instead, it
%% simply configures AAE to build/rebuild and run exchanges between the data
%% partitions. It then performs direct vnode reads on all replicas and verify
%% they eventually match.
%%
%% Data recovery after the following scenarios is tested:
%%
%% - Data for a partition completely disappears.
%% - Less than N replicas are written
%% - Less than N replicas are updated
%%
%% Also, a sanity check is done to make sure AAE repairs go away eventually
%% if there is no activity.  That was an actual early AAE bug.

-module(verify_aae).
-export([confirm/0, verify_aae/1, test_single_partition_loss/3]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(AAE_THROTTLE_LIMITS, [{-1, 0}, {100, 10}]).
-define(CFG,
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {on, []}},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 100},
           {anti_entropy_expire, 24 * 60 * 60 * 1000}, % Not for now!
           {anti_entropy_tick, 500},
           {aae_throttle_limits, ?AAE_THROTTLE_LIMITS}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 3).
-define(NUM_KEYS, 1000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).

confirm() ->
    Nodes = rt:build_cluster(?NUM_NODES, ?CFG),
    verify_throttle_config(Nodes),
    verify_aae(Nodes),
    pass.

verify_throttle_config(Nodes) ->
    lists:foreach(
      fun(Node) ->
              ?assert(rpc:call(Node,
                               riak_kv_entropy_manager,
                               is_aae_throttle_enabled,
                               [])),
              ?assertMatch(?AAE_THROTTLE_LIMITS,
                           rpc:call(Node,
                                    riak_kv_entropy_manager,
                                    get_aae_throttle_limits,
                                    []))
      end,
      Nodes).

verify_aae(Nodes) ->
    Node1 = hd(Nodes),
    % First, recovery without tree rebuilds

    % Test recovery from to few replicas written
    KV1 = test_data(1, 1000),
    test_less_than_n_writes(Node1, KV1),

    % Test recovery when replicas are different
    KV2 = [{K, <<V/binary, "a">>} || {K, V} <- KV1],
    test_less_than_n_mods(Node1, KV2),

    lager:info("Run similar tests now with tree rebuilds enabled"),
    start_tree_rebuilds(Nodes),

    % Test recovery from too few replicas written
    KV3 = test_data(1001, 2000),
    test_less_than_n_writes(Node1, KV3),

    % Test recovery when replicas are different
    KV4 = [{K, <<V/binary, "a">>} || {K, V} <- KV3],
    test_less_than_n_mods(Node1, KV4),

    lager:info("Writing 1000 objects"),
    KV5 = test_data(2001, 3000),
    write_data(Node1, KV5),

    % Test recovery from single partition loss.
    {PNuke, NNuke} = choose_partition_to_nuke(Node1, ?BUCKET, KV5),
    test_single_partition_loss(NNuke, PNuke, KV5),

    % Test recovery from losing AAE data
    test_aae_partition_loss(NNuke, PNuke, KV5),

    % Test recovery from losing both AAE and KV data
    test_total_partition_loss(NNuke, PNuke, KV5),

    % Make sure AAE repairs die down.
    wait_until_no_aae_repairs(Nodes),

    % Verify that AAE eventually upgrades to version 0(or already has)
    wait_until_hashtree_upgrade(Nodes),

    lager:info("Finished verifying AAE magic"),
    ok.

start_tree_rebuilds(Nodes) ->
    rpc:multicall(Nodes, application, set_env, [riak_kv, anti_entropy_expire,
                                                15 * 1000]).

acc_preflists(Pl, PlCounts) ->
    lists:foldl(fun(Idx, D) ->
                        dict:update(Idx, fun(V) -> V+1 end, 0, D)
                end, PlCounts, Pl).

choose_partition_to_nuke(Node, Bucket, KVs) ->
    Preflists = [get_preflist(Node, Bucket, K) || {K, _} <- KVs],
    PCounts = lists:foldl(fun acc_preflists/2, dict:new(), Preflists),
    CPs = [{C, P} || {P, C} <- dict:to_list(PCounts)],
    {_, MaxP} = lists:max(CPs),
    MaxP.

get_preflist(Node, B, K) ->
    DocIdx = rpc:call(Node, riak_core_util, chash_key, [{B, K}]),
    PlTagged = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, ?N_VAL, riak_kv]),
    Pl = [E || {E, primary} <- PlTagged],
    Pl.

to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, ?BUCKET, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(?BUCKET, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.

% @doc Verifies that the data is eventually restored to the expected set.
verify_data(Node, KeyValues) ->
    lager:info("Verify all replicas are eventually correct"),
    PB = rt:pbc(Node),
    CheckFun =
    fun() ->
            Matches = [verify_replicas(Node, ?BUCKET, K, V, ?N_VAL)
                       || {K, V} <- KeyValues],
            CountTrues = fun(true, G) -> G+1; (false, G) -> G end,
            NumGood = lists:foldl(CountTrues, 0, Matches),
            Num = length(KeyValues),
            case Num == NumGood of
                true -> true;
                false ->
                    lager:info("Data not yet correct: ~p mismatches",
                               [Num-NumGood]),
                    false
            end
    end,
    MaxTime = rt_config:get(rt_max_wait_time),
    Delay = 2000, % every two seconds until max time.
    Retry = MaxTime div Delay,
    case rt:wait_until(CheckFun, Retry, Delay) of
        ok ->
            lager:info("Data is now correct. Yay!");
        fail ->
            lager:error("AAE failed to fix data"),
            ?assertEqual(aae_fixed_data, aae_failed_to_fix_data)
    end,
    riakc_pb_socket:stop(PB),
    ok.

merge_values(O) ->
    Vals = riak_object:get_values(O),
    lists:foldl(fun(NV, V) ->
                        case size(NV) > size(V) of
                            true -> NV;
                            _ -> V
                        end
                end, <<>>, Vals).

verify_replicas(Node, B, K, V, N) ->
    Replies = [rt:get_replica(Node, B, K, I, N)
               || I <- lists:seq(1,N)],
    Vals = [merge_values(O) || {ok, O} <- Replies],
    Expected = [V || _ <- lists:seq(1, N)],
    Vals == Expected.

test_single_partition_loss(Node, Partition, KeyValues)
  when is_atom(Node), is_integer(Partition) ->
    lager:info("Verify recovery from the loss of partition ~p", [Partition]),
    wipe_out_partition(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data(Node, KeyValues).

test_aae_partition_loss(Node, Partition, KeyValues)
  when is_atom(Node), is_integer(Partition) ->
    lager:info("Verify recovery from the loss of AAE data for partition ~p", [Partition]),
    wipe_out_aae_data(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data(Node, KeyValues).

test_total_partition_loss(Node, Partition, KeyValues)
  when is_atom(Node), is_integer(Partition) ->
    lager:info("Verify recovery from the loss of AAE and KV data for partition ~p", [Partition]),
    wipe_out_partition(Node, Partition),
    wipe_out_aae_data(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data(Node, KeyValues).

test_less_than_n_writes(Node, KeyValues) ->
    lager:info("Writing ~p objects with N=1, AAE should ensure they end up"
               " with ~p replicas", [length(KeyValues), ?N_VAL]),
    write_data(Node, KeyValues, [{n_val, 1}]),
    verify_data(Node, KeyValues).

test_less_than_n_mods(Node, KeyValues) ->
    lager:info("Modifying only one replica for ~p objects. AAE should ensure"
               " all replicas end up modified", [length(KeyValues)]),
    write_data(Node, KeyValues, [{n_val, 1}]),
    verify_data(Node, KeyValues).

wipe_out_partition(Node, Partition) ->
    lager:info("Wiping out partition ~p in node ~p", [Partition, Node]),
    rt:clean_data_dir(Node, dir_for_partition(Partition)),
    ok.

wipe_out_aae_data(Node, Partition) ->
    lager:info("Wiping out AAE data for partition ~p in node ~p", [Partition, Node]),
    rt:clean_data_dir(Node, "anti_entropy/"++integer_to_list(Partition)),
    ok.

base_dir_for_backend(undefined) ->
    base_dir_for_backend(bitcask);
base_dir_for_backend(bitcask) ->
    "bitcask";
base_dir_for_backend(eleveldb) ->
    "leveldb".

restart_vnode(Node, Service, Partition) ->
    VNodeName = list_to_atom(atom_to_list(Service) ++ "_vnode"),
    {ok, Pid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                         [Partition, VNodeName]),
    ?assert(rpc:call(Node, erlang, exit, [Pid, kill_for_test])),
    Mon = monitor(process, Pid),
    receive
        {'DOWN', Mon, _, _, _} ->
            ok
    after
        rt_config:get(rt_max_wait_time) ->
            lager:error("VNode for partition ~p did not die, the bastard",
                        [Partition]),
            ?assertEqual(vnode_killed, {failed_to_kill_vnode, Partition})
    end,
    {ok, NewPid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                            [Partition, VNodeName]),
    lager:info("Vnode for partition ~p restarted as ~p",
               [Partition, NewPid]).

dir_for_partition(Partition) ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    BaseDir = base_dir_for_backend(KVBackend),
    filename:join([BaseDir, integer_to_list(Partition)]).

% @doc True if the AAE stats report zero data repairs for last exchange
% across the board.
wait_until_no_aae_repairs(Nodes) ->
    lager:info("Verifying AAE repairs go away without activity"),
    rt:wait_until(fun() -> no_aae_repairs(Nodes) end).

no_aae_repairs(Nodes) when is_list(Nodes) ->
    MaxCount = max_aae_repairs(Nodes),
    lager:info("Max AAE repair count across the board is ~p", [MaxCount]),
    MaxCount == 0.

max_aae_repairs(Nodes) when is_list(Nodes) ->
    MaxCount = lists:max([max_aae_repairs(Node) || Node <- Nodes]),
    MaxCount;
max_aae_repairs(Node) when is_atom(Node) ->
    Info = rpc:call(Node, riak_kv_entropy_info, compute_exchange_info, []),
    LastCounts = [Last || {_, _, _, {Last, _, _, _}} <- Info],
    MaxCount = lists:max(LastCounts),
    MaxCount.

wait_until_hashtree_upgrade(Nodes) ->
    lager:info("Verifying AAE hashtrees eventually all upgrade to version 0"),
    rt:wait_until(fun() -> all_hashtrees_upgraded(Nodes) end).

all_hashtrees_upgraded(Nodes) when is_list(Nodes) ->
    [Check|_] = lists:usort([all_hashtrees_upgraded(Node) || Node <- Nodes]),
    Check;

all_hashtrees_upgraded(Node) when is_atom(Node) ->
    case rpc:call(Node, riak_kv_entropy_manager, get_version, []) of
        0 ->
            Trees = rpc:call(Node, riak_kv_entropy_manager, get_trees_version, []),
            case [Idx || {Idx, undefined} <- Trees] of
                [] ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
     end.
    
