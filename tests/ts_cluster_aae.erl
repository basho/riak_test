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

-module(ts_cluster_aae).
-export([confirm/0, verify_aae_ts/1, test_single_partition_loss_ts/6]).
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
-define(TABLE, <<"GeoCheckin">>).
-define(N_VAL, 3).

confirm() ->
    Nodes = rt:build_cluster(?NUM_NODES, ?CFG),
    verify_throttle_config(Nodes),
    verify_aae_ts(Nodes),
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

verify_aae_ts(Nodes) ->
    DDL = ts_data:get_ddl(),
    ts_setup:create_bucket_type(Nodes, DDL, ?TABLE),
    ts_setup:activate_bucket_type(Nodes, ?TABLE),

    Node1 = hd(Nodes),

    % Verify that AAE eventually upgrades to version 0(or already has)
    wait_until_hashtree_upgrade(Nodes), 
    
    % Recovery without tree rebuilds

    % Test recovery from to few replicas written
    KV1 = test_data_ts(1, 1000),
    test_less_than_n_writes_ts(Node1, KV1, ?TABLE, ?N_VAL, <<"weather">>, [{n_val, 1}]),

    % Test recovery when replicas are different
    KV2 = [{A, B, C, <<D/binary, "a">>, E} || {A, B, C, D, E} <- KV1],
    test_less_than_n_mods_ts(Node1, KV2, ?TABLE, ?N_VAL, <<"weather">>, [{n_val, 1}]),

    lager:info("Run similar tests now with tree rebuilds enabled"),
    start_tree_rebuilds_ts(Nodes),

    % Test recovery from too few replicas written
    KV3 = test_data_ts(1001, 2000),
    test_less_than_n_writes_ts(Node1, KV3, ?TABLE, ?N_VAL, <<"weather">>, [{n_val, 1}]),

    % Test recovery when replicas are different
    KV4 = [{A, B, C, <<D/binary, "a">>, E} || {A, B, C, D, E} <- KV3],
    test_less_than_n_mods_ts(Node1, KV4, ?TABLE, ?N_VAL, <<"weather">>, [{n_val, 1}]),

    lager:info("Writing 1000 objects"),
    KV5 = test_data_ts(2001, 3000),
    write_data_ts(Node1, KV5, ?TABLE),

    % Test recovery from single partition loss.
    {PNuke, NNuke} = choose_partition_to_nuke_ts(Node1, ?TABLE, KV5, ?N_VAL),
    test_single_partition_loss_ts(NNuke, PNuke, KV5, ?TABLE, ?N_VAL, <<"weather">>),

    % Test recovery from losing AAE data
    test_aae_partition_loss_ts(NNuke, PNuke, KV5, ?TABLE, ?N_VAL, <<"weather">>),

    % Test recovery from losing both AAE and KV data
    test_total_partition_loss_ts(NNuke, PNuke, KV5, ?TABLE, ?N_VAL, <<"weather">>),

    % Make sure AAE repairs die down.
    wait_until_no_aae_repairs(Nodes),

    lager:info("Finished verifying TS AAE magic"),
    ok.

start_tree_rebuilds_ts(Nodes) ->
    rpc:multicall(Nodes, application, set_env, [riak_kv, anti_entropy_expire,
                                                15 * 1000]).

acc_preflists(Pl, PlCounts) ->
    lists:foldl(fun(Idx, D) ->
                        dict:update(Idx, fun(V) -> V+1 end, 0, D)
                end, PlCounts, Pl).

test_data_ts(Start, End) ->
    StepSize = 1000*60*5,
    ts_data:get_valid_select_data(fun() -> lists:seq(1 + StepSize*Start, 1 + StepSize*End, StepSize) end).

key_ts(Data) -> 
    lists:sublist(tuple_to_list(Data), 3).

value_ts(Data) -> 
    lists:sublist(tuple_to_list(Data), 4, 10).

write_data_ts(Node, KVs, Table) ->
    write_data_ts(Node, KVs, Table, []).

write_data_ts(Node, KVs, Table, _Opts) ->
    [begin
         O =
         case ts_ops:get([Node], Table, key_ts(TS_Data_Tuple)) of
             {ok, {_Cols, _DataRowInList}} ->
                 [TS_Data_Tuple];
             _ ->
                 [TS_Data_Tuple]
         end,
         ?assertMatch(ok, ts_ops:put([Node], Table, O))
     end || TS_Data_Tuple <- KVs],
    ok.

% @doc Verifies that the data is eventually restored to the expected set.
verify_data_ts(Node, KeyValues, Table, Nval, MergeField) ->
    lager:info("Verify all replicas are eventually correct"),
    CheckFun =
    fun() ->
            Matches = [verify_replicas_ts(Node, Table, key_ts(TS_Data_Tuple), value_ts(TS_Data_Tuple), Nval, MergeField)
                       || TS_Data_Tuple <- KeyValues],
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
    ok.
%<<"weather">> nthfield
%using weather field binary
merge_values_ts(O, Nth, NthField) ->
    Vals = riak_object:get_values(O),
    lists:foldl(fun(NV, V) ->
                        {NthField,NthVal} = lists:nth(Nth,NV),
                        case size(NthVal) > size(V) of
                            true -> NV;
                            _ -> V
                        end
                end, <<>>, Vals).

%mergefield <<"weather">>
verify_replicas_ts(Node, B, K, V, N, MergeField) ->
    Replies = [get_replica_ts(Node, B, K, I, N)
               || I <- lists:seq(1,N)],
    Vals = [merge_values_ts(O, 4, MergeField) || {ok, O} <- Replies],
    ModifiedVals = [extract_values_ts(AVal, [4,5]) || AVal <- Vals],
    Expected = [V || _ <- lists:seq(1, N)],
    ModifiedVals == Expected.

extract_values_ts(AList, IdxList) ->
    TupleVals = [lists:nth(Idx, AList) || Idx <- IdxList],
    ExtractedVals = [Val || {_Field, Val} <- TupleVals],
    ExtractedVals.

get_replica_ts(Node, Table, Key, I, N) ->
    PKey = get_partition_key_ts(Node, Table, Key),
    LKey = get_local_key_ts(Node, Table, Key),
    Pl = get_preflist_ts(Node, PKey, N),
    {Partition, PNode} = lists:nth(I, Pl),
    Ref = Reqid = make_ref(),
    Sender = {raw, Ref, self()},
    rpc:call(PNode, riak_kv_vnode, get,
             [{Partition, PNode}, LKey, Ref, Sender]),
    receive
        {Ref, {r, Result, _, Reqid}} ->
            Result;
        {Ref, Reply} ->
            Reply
    after
        60000 ->
            lager:error("Replica ~p get for ~p/~p timed out",
                        [I, Table, Key]),
            ?assert(false)
    end.

get_partition_key_ts(Node, Table, K) ->
    KeyTuple = list_to_tuple(K),
    {ok, Mod, DDL} = rpc:call(Node, riak_kv_ts_util, get_table_ddl, [Table]),
    PK = rpc:call(Node, riak_ql_ddl, get_partition_key, [DDL, KeyTuple, Mod]),
    PKeyTuple = rpc:call(Node, riak_kv_ts_util, encode_typeval_key, [PK]),
    PKey = {{Table, Table}, PKeyTuple},
    PKey.

get_local_key_ts(Node, Table, K) ->
    KeyTuple = list_to_tuple(K),
    {Fam, Ser, TS} = KeyTuple,
    LK = [{varchar, Fam},{varchar, Ser},{timestamp, TS}],
    LKeyTuple = rpc:call(Node, riak_kv_ts_util, encode_typeval_key, [LK]),
    LKey = {{Table, Table}, LKeyTuple},
    LKey.

get_preflist_ts(Node, PKey, Nval) ->
    DocIdx = rpc:call(Node, riak_core_util, chash_key, [PKey]),
    PlTagged = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, Nval, riak_kv]),
    Pl = [E || {E, primary} <- PlTagged],
    Pl.

choose_partition_to_nuke_ts(Node, Table, KVs, Nval) ->
    Preflists = [get_preflist_ts(Node, get_partition_key_ts(Node, Table,key_ts(TS_Data_Tuple)), Nval) || TS_Data_Tuple <- KVs],
    PCounts = lists:foldl(fun acc_preflists/2, dict:new(), Preflists),
    CPs = [{C, P} || {P, C} <- dict:to_list(PCounts)],
    {_, MaxP} = lists:max(CPs),
    MaxP.

test_single_partition_loss_ts(Node, Partition, KeyValues, Table, Nval, MergeField)
  when is_atom(Node), is_integer(Partition) ->
    lager:info("Verify recovery from the loss of partition ~p", [Partition]),
    wipe_out_partition(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data_ts(Node, KeyValues, Table, Nval, MergeField).

test_aae_partition_loss_ts(Node, Partition, KeyValues, Table, Nval, MergeField)
  when is_atom(Node), is_integer(Partition) ->
    lager:info("Verify recovery from the loss of AAE data for partition ~p", [Partition]),
    wipe_out_aae_data(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data_ts(Node, KeyValues, Table, Nval, MergeField).

test_total_partition_loss_ts(Node, Partition, KeyValues, Table, Nval, MergeField)
  when is_atom(Node), is_integer(Partition) ->
    lager:info("Verify recovery from the loss of AAE and KV data for partition ~p", [Partition]),
    wipe_out_partition(Node, Partition),
    wipe_out_aae_data(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data_ts(Node, KeyValues, Table, Nval, MergeField).

%opts [{n_val, 1}]
test_less_than_n_writes_ts(Node, KeyValues, Table, Nval, MergeField, Opts) ->
    lager:info("Writing ~p objects with N=1, AAE should ensure they end up"
               " with ~p replicas", [length(KeyValues), Nval]),
    write_data_ts(Node, KeyValues, Table, Opts),
    verify_data_ts(Node, KeyValues, Table, Nval, MergeField).


test_less_than_n_mods_ts(Node, KeyValues, Table, Nval, MergeField, Opts) ->
    lager:info("Modifying only one replica for ~p objects. AAE should ensure"
               " all replicas end up modified", [length(KeyValues)]),
    write_data_ts(Node, KeyValues, Table, Opts),
    verify_data_ts(Node, KeyValues, Table, Nval, MergeField).

wipe_out_partition(Node, Partition) ->
    lager:info("Wiping out partition ~p in node ~p", [Partition, Node]),
    rt:clean_data_dir(Node, dir_for_partition(Partition)),
    ok.

wipe_out_aae_data(Node, Partition) ->
    lager:info("Wiping out AAE data for partition ~p in node ~p", [Partition, Node]),
    rt:clean_data_dir(Node, "anti_entropy/"++integer_to_list(Partition)),
    ok.

base_dir_for_backend() ->
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
    BaseDir = base_dir_for_backend(),
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
