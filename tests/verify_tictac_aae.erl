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

-module(verify_tictac_aae).
-export([confirm/0, verify_aae_norebuild/1, verify_aae_rebuild/1, test_single_partition_loss/3]).
-export([verify_data/2]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 16).
-define(AAE_THROTTLE_LIMITS, [{-1, 0}, {100, 10}]).
-define(CFG_NOREBUILD(PrimaryOnly, InitialSkip, MaxResults, ExTick, KR),
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, ExTick * 1000},
           {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
           {tictacaae_primaryonly, PrimaryOnly},
           {tictacaae_stepinitialtick, InitialSkip},
           {tictacaae_maxresults, MaxResults},
           {tictacaae_repairloops, 4},
           {tictacaae_enablekeyrange, KR}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(CFG_REBUILD,
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 0},
           {tictacaae_rebuilddelay, 60},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, 15 * 1000}, % Check for rebuilds!
           {max_aae_queue_time, 0},
           {tictacaae_stepinitialtick, false},
           {log_readrepair, true},
           {tictacaae_enablekeyrange, true}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 4).
-define(NUM_KEYS, 2000).
-define(BUCKET, <<"test_bucket">>).
-define(ALT_BUCKET1, <<"alt_bucket1">>).
-define(ALT_BUCKET2, <<"alt_bucket2">>).
-define(ALT_BUCKET3, <<"alt_bucket3">>).
-define(ALT_BUCKET4, <<"alt_bucket4">>).
-define(N_VAL, 3).
-define(STATS_DELAY, 1000).

confirm() ->

    lager:info("Test with no rebuilds - and no startup skip and no key ranges"),
    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(true, false, 64, 15, false)),
    ok = verify_aae_norebuild(Nodes1, true),
    rt:clean_cluster(Nodes1),

    lager:info("Test with no rebuilds - but with startup skip and key ranges"),
    Nodes2 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(true, true, 64, 10, true)),
    ok = verify_aae_norebuild(Nodes2, false),
    rt:clean_cluster(Nodes2),

    lager:info("Test with rebuilds"),
    Nodes3 = rt:build_cluster(?NUM_NODES, ?CFG_REBUILD),
    ok = verify_aae_rebuild(Nodes3),
    rt:clean_cluster(Nodes3),

    lager:info("Test with no rebuilds - and AAE on fallbacks"),
    Nodes4 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(false, false, 128, 10, false)),
    ok = verify_aae_norebuild(Nodes4, false),
    rt:clean_cluster(Nodes4),

    OldVsn = previous,
    lager:info("Building previous version cluster ~p", [OldVsn]),
    [Nodes5] = 
        rt:build_clusters([{?NUM_NODES, OldVsn, ?CFG_NOREBUILD(true, false, 64, 15, false)}]),

    [NodeToUpgrade|_Rest] = Nodes5,

    {riak_kv, _, RiakVer} =
        lists:keyfind(riak_kv,
            1,
            rpc:call(NodeToUpgrade, application, loaded_applications, [])),
    
    UpgradeRE = "riak_kv\-3\.0\.[0-8]$",
    case re:run(RiakVer, UpgradeRE) of
        nomatch ->
            lager:info("Skipping upgrade test - previous ~s > 3.0.8", [RiakVer]),
            pass;
        _ ->
            lager:info("Running upgrade test with previous version ~s", [RiakVer]),
            rt:upgrade(NodeToUpgrade, current),
            rt:wait_for_service(NodeToUpgrade, riak_kv),

            ?assertNot(check_capability(NodeToUpgrade)),

            ok = verify_aae_norebuild(Nodes5, false),

            CheckFun = 
                fun(StatName) ->
                    proplists:get_value(StatName,
                        verify_riak_stats:get_stats(NodeToUpgrade, ?STATS_DELAY))
                end,
            ?assertEqual(0, CheckFun(<<"tictacaae_bucket_total">>)),
            ?assertEqual(0, CheckFun(<<"tictacaae_modtime_total">>)),
            ?assertNotEqual(0, CheckFun(<<"tictacaae_exchange_total">>)),

            pass
    end.


check_capability(Node) ->
    rpc:call(Node,
        riak_core_capability,
        get,
        [{riak_kv, tictacaae_prompted_repairs}, false]).

verify_aae_norebuild(Nodes) ->
    verify_aae_norebuild(Nodes, false).

verify_aae_norebuild(Nodes, CheckTypeStats) ->
    lager:info("Tictac AAE tests without rebuilding trees"),
    Node1 = hd(Nodes),

    % Recovery without tree rebuilds

    % Test recovery from too few replicas written
    KV1 = test_data(1, ?NUM_KEYS),
    test_less_than_n_writes(Node1, KV1),

    RepSN = <<"read_repairs_total">>,
    Repairs =
        lists:sum(
            lists:map(
                fun(N) ->
                    proplists:get_value(RepSN,
                        verify_riak_stats:get_stats(N, ?STATS_DELAY))
                end,
                Nodes)),
    ?assert(Repairs >= ?NUM_KEYS),
    ?assert(Repairs =< ?NUM_KEYS + 1),

    KV2 = [{K, <<V/binary, "a">>} || {K, V} <- KV1],
    lager:info("Writing additional n=1 data to require more repairs"),
    write_data(Node1, KV2, [{n_val, 1}], ?ALT_BUCKET1),
    write_data(Node1, KV2, [{n_val, 1}], ?ALT_BUCKET2),
    write_data(Node1, KV2, [{n_val, 1}], ?ALT_BUCKET3),
    write_data(Node1, KV2, [{n_val, 1}], ?ALT_BUCKET4),
    lager:info("Updating data on n=1"),
    test_less_than_n_mods(Node1, KV2),
    lager:info("Verifying alternative bucket data"),
    verify_data(Node1, KV2, ?ALT_BUCKET1),
    verify_data(Node1, KV2, ?ALT_BUCKET2),
    verify_data(Node1, KV2, ?ALT_BUCKET3),
    verify_data(Node1, KV2, ?ALT_BUCKET4),

    case CheckTypeStats of
        true ->
            B_SN = <<"tictacaae_bucket_total">>,
            MT_SN = <<"tictacaae_modtime_total">>,
            E_SN = <<"tictacaae_exchange_total">>,
            VerifyFun = 
                fun(StatName) ->
                    fun(Node) ->
                        V = proplists:get_value(StatName,
                                verify_riak_stats:get_stats(Node, ?STATS_DELAY)),
                        ?assertNotEqual(0, V)
                    end
                end,

            lists:foreach(VerifyFun(B_SN), Nodes),
            lists:foreach(VerifyFun(MT_SN), Nodes),
            lists:foreach(VerifyFun(E_SN), Nodes),
            ok;
        false ->
            ok
    end,

    ok.

verify_aae_rebuild(Nodes) ->
    lager:info("Tictac AAE tests with rebuilding trees"),
    Node1 = hd(Nodes),

    % Test recovery from too few replicas written
    KV1 = test_data(1, ?NUM_KEYS),
    test_less_than_n_writes(Node1, KV1),

    % Test recovery when replicas are different
    KV2 = [{K, <<V/binary, "a">>} || {K, V} <- KV1],
    test_less_than_n_mods(Node1, KV2),

    % Test recovery from too few replicas written
    KV3 = test_data(?NUM_KEYS + 1, 2 * ?NUM_KEYS),
    test_less_than_n_writes(Node1, KV3),

    % Test recovery when replicas are different
    KV4 = [{K, <<V/binary, "a">>} || {K, V} <- KV3],
    test_less_than_n_mods(Node1, KV4),

    lager:info("Writing ~w objects", [?NUM_KEYS]),
    KV5 = test_data(1 + 2 * ?NUM_KEYS, 3 * ?NUM_KEYS),
    write_data(Node1, KV5),

    % Test recovery from single partition loss.
    {PNuke, NNuke} = choose_partition_to_nuke(Node1, ?BUCKET, KV5),
    test_single_partition_loss(NNuke, PNuke, KV5),

    % Test recovery from losing AAE data
    test_aae_partition_loss(NNuke, PNuke, KV5),

    % Test recovery from losing both AAE and KV data
    test_total_partition_loss(NNuke, PNuke, KV5),

    lager:info("Finished verifying AAE magic"),
    ok.


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
    list_to_binary(io_lib:format("K~8..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    write_data(Node, KVs, Opts, ?BUCKET).

write_data(Node, KVs, Opts, Bucket) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, Bucket, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(Bucket, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.

% @doc Verifies that the data is eventually restored to the expected set.
verify_data(Node, KeyValues) ->
    verify_data(Node, KeyValues, ?BUCKET).

verify_data(Node, KeyValues, Bucket) ->
    lager:info("Verify all replicas are eventually correct"),
    PB = rt:pbc(Node),
    CheckFun =
    fun() ->
            Matches = [verify_replicas(Node, Bucket, K, V, ?N_VAL)
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
    ok = 
        case rt:wait_until(CheckFun, Retry, Delay) of
            ok ->
                lager:info("Data is now correct. Yay!");
            fail ->
                lager:error("AAE failed to fix data"),
                aae_failed_to_fix_data
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
    rt:clean_data_dir(Node, "tictac_aae/"++integer_to_list(Partition)),
    ok.

base_dir_for_backend(leveled) ->
    "leveled";
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


