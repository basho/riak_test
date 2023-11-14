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
%% @doc
%% Coordinate the reaping of tombs between replicating clusters

-module(verify_dollarkeyreap).
-behavior(riak_test).
-export([confirm/0]).
-export([read_from_cluster/6]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 8).
-define(A_NVAL, 2).
-define(B_NVAL, 2).

-define(KEY_COUNT, 20000).

-define(LOOP_COUNT, 10).
-define(TOMB_PAUSE, 2).

-define(SNK_WORKERS, 4).

-define(DELETE_WAIT, 8000).
%% This must be increased, otherwise tombstones may be reaped before their
%% presence can be checked in the test

-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(CONFIG(RingSize, NVal, DeleteMode, CountTombs), [
    {riak_core,
        [
         {ring_creation_size, RingSize},
         {default_bucket_props,
             [
                 {n_val, NVal},
                 {allow_mult, true},
                 {dvv_enabled, true}
             ]}
        ]
    },
    {riak_kv,
      [
        {anti_entropy, {off, []}},
        {tictacaae_active, active},
        {tictacaae_parallelstore, leveled_ko},
            % if backend not leveled will use parallel key-ordered
            % store
        {tictacaae_storeheads, true},
        {tictacaae_rebuildwait, 4},
        {tictacaae_rebuilddelay, 3600},
        {tictacaae_exchangetick, 3600 * 1000}, % don't exchange during test
        {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
        {ttaaefs_maxresults, 128},
        {repl_reap, true},
        {log_snk_stacktrace, true},
        {tombstone_pause, ?TOMB_PAUSE},
        {delete_mode, DeleteMode},
        {dollarkey_readtombs, CountTombs}
      ]}
    ]).

-define(REPL_CONFIG(LocalClusterName, PeerList, SrcQueueDefns), [
{riak_kv,
    [
        {replrtq_srcqueue, SrcQueueDefns},
        {replrtq_enablesink, true},
        {replrtq_enablesrc, true},
        {replrtq_sinkqueue, LocalClusterName},
        {replrtq_sinkpeers, PeerList},
        {replrtq_sinkworkers, ?SNK_WORKERS}
    ]}
]).


repl_config(RemoteCluster, LocalClusterName, PeerList) ->
    ?REPL_CONFIG(
        LocalClusterName, PeerList, atom_to_list(RemoteCluster) ++ ":any").


confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),

    case KVBackend of
        leveled ->
            [ClusterA, ClusterB] =
                rt:deploy_clusters([
                    {3, ?CONFIG(?A_RING, ?A_NVAL, keep, true)},
                    {3, ?CONFIG(?B_RING, ?B_NVAL, keep, false)}]),

            lager:info("***************************************************"),
            lager:info("Test $key index with deletes and reaps"),
            lager:info("***************************************************"),
            test_dollarkey(ClusterA, ClusterB, pb);
        KVBackend ->
            lager:info(
                "Test ignored as not relevant to backend ~w",
                [KVBackend]),
            pass
    end.

test_dollarkey(ClusterA, ClusterB, Protocol) ->

    [NodeA1, NodeA2, NodeA3] = ClusterA,
    [NodeB1, NodeB2, NodeB3] = ClusterB,

    FoldToPeerConfig = 
        fun(Node, Acc) ->
            {Protocol, {IP, Port}} =
                lists:keyfind(Protocol, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port)
                ++ ":" ++ atom_to_list(Protocol)
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA),

    ACfg = repl_config(cluster_b, cluster_a, ClusterASnkPL),
    BCfg = repl_config(cluster_a, cluster_b, ClusterBSnkPL),
    rt:set_advanced_conf(NodeA1, ACfg),
    rt:set_advanced_conf(NodeA2, ACfg),
    rt:set_advanced_conf(NodeA3, ACfg),
    rt:set_advanced_conf(NodeB1, BCfg),
    rt:set_advanced_conf(NodeB2, BCfg),
    rt:set_advanced_conf(NodeB3, BCfg),

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end, ClusterA ++ ClusterB),

    PBCa = rt:pbc(NodeA1),
    PBCb = rt:pbc(NodeB1),

    write_to_cluster(NodeA1, ?TEST_BUCKET, 1, ?KEY_COUNT, new_obj),
    {ok, {index_results_v1, R0, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCa, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(100)),
    ?assertEqual(100, length(R0)),

    rt:wait_until(
        fun() ->
            {ok, {index_results_v1, RCheck, undefined, undefined}} =
                riakc_pb_socket:get_index_range(
                    PBCb, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(?KEY_COUNT)),
            length(RCheck) == ?KEY_COUNT
        end
    ),

    SWa = os:timestamp(),
    {ok, {index_results_v1, RA0, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCa, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(?KEY_COUNT)),
    SWb = os:timestamp(),
    {ok, {index_results_v1, RB0, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCb, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(?KEY_COUNT)),
    SW = os:timestamp(),
    lager:info(
        "Results on A ~w in ~w ms and B ~w in ~w ms",
        [length(RA0), timer:now_diff(SWb, SWa) div 1000,
            length(RB0), timer:now_diff(SW, SWb) div 1000]),

    delete_from_cluster(NodeA1, ?TEST_BUCKET, 1, 50),
    {ok, {index_results_v1, RA1, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCa, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(100)),
    ?assertEqual(100, length(RA1)),
    rt:wait_until(
        fun() ->
            {ok, {index_results_v1, RB1, undefined, undefined}} =
                riakc_pb_socket:get_index_range(
                    PBCb, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(100)),
            lager:info(
                "$key query on ClusterB returns ~w expected 50",
                [length(RB1)]),
            50 == length(RB1)
        end
    ),
    lager:info("Confirm term regex still behaves as expected"),
    {ok, {index_results_v1, RA2, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCa, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(100),
            [{term_regex, ".*4$"}]),
    ?assertEqual(10, length(RA2)),
    {ok, {index_results_v1, RB2, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCb, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(100),
            [{term_regex, ".*4$"}]),
    ?assertEqual(5, length(RB2)),

    lager:info("Find keys will return tombstones"),
    ?assertEqual(20000, find_keys(NodeA1, ?TEST_BUCKET, all, all)),

    delete_from_cluster(NodeA1, ?TEST_BUCKET, 51, ?KEY_COUNT),
    reap_from_cluster(NodeA1, local, ?TEST_BUCKET),

    lager:info("Confirm all keys reaped from both clusters"),
    rt:wait_until(
        fun() ->
            {ok, 0} == find_tombs(NodeA1, ?TEST_BUCKET, all, all, return_count)
        end),
    lager:info("All reaped from Cluster A"),
    lager:info("Now would expect ClusterB to be eventually in sync"),
    rt:wait_until(
        fun() ->
            {ok, 0} == find_tombs(NodeB1, ?TEST_BUCKET, all, all, return_count)
        end),

    {ok, {index_results_v1, RA3, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCa, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(100)),
    ?assertEqual(0, length(RA3)),
    {ok, {index_results_v1, RB3, undefined, undefined}} =
        riakc_pb_socket:get_index_range(
            PBCb, ?TEST_BUCKET, <<"$key">>, to_key(1), to_key(100)),
    ?assertEqual(0, length(RB3)),
    
    pass.


to_key(N) ->
    list_to_binary(io_lib:format("~8..0B", [N])).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Bucket, Start, End, CommonValBin) -> 
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Obj = 
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMMON_VAL_INIT,
                        riak_object:new(
                            Bucket, to_key(N), <<N:32/integer, CVB/binary>>);
                    UpdateBin ->
                        UPDV = <<N:32/integer, UpdateBin/binary>>,
                        {ok, PrevObj} = riak_client:get(Bucket, to_key(N), C),
                        riak_object:update_value(PrevObj, UPDV)
                end,
            try riak_client:put(Obj, C) of
                ok ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            catch
                What:Why ->
                    [{N, {What, Why}} | Acc]
            end
        end,
    Errors = lists:foldl(F, [], lists:seq(Start, End)),
    lager:warning("~p errors while writing: ~p", [length(Errors), Errors]),
    ?assertEqual([], Errors).

delete_from_cluster(Node, Bucket, Start, End) ->
    lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B", [N])),
            try riak_client:delete(Bucket, Key, C) of
                ok ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            catch
                What:Why ->
                    [{N, {What, Why}} | Acc]
            end
        end,
    Errors = lists:foldl(F, [], lists:seq(Start, End)),
    lager:warning("~p errors while deleting: ~p", [length(Errors), Errors]),
    ?assertEqual([], Errors).


reap_from_cluster(Node, local, Bucket) ->
    lager:info("Auto-reaping found tombs from node ~p", [Node]),
    {ok, C} = riak:client_connect(Node),
    Query = {reap_tombs, Bucket, all, all, all, local},
    {ok, Count} = riak_client:aae_fold(Query, C),
    ?assertEqual(?KEY_COUNT, Count).
    

read_from_cluster(Node, Bucket, Start, End, CommonValBin, Errors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B", [N])),
            case riak_client:get(Bucket, Key, C) of
                {ok, Obj} ->
                    ExpectedVal = <<N:32/integer, CommonValBin/binary>>,
                    case riak_object:get_values(Obj) of
                        [ExpectedVal] ->
                            Acc;
                        Siblings when length(Siblings) > 1 ->
                            lager:info(
                                "Siblings for Key ~s:~n ~w", [Key, Obj]),
                            [{wrong_value, Key, siblings}|Acc];
                        [UnexpectedVal] ->
                            [{wrong_value, Key, UnexpectedVal}|Acc]
                    end;
                {error, Error} ->
                    [{fetch_error, Error, Key}|Acc]
            end
        end,
    ErrorsFound = lists:foldl(F, [], lists:seq(Start, End)),
    case Errors of
        undefined ->
            lager:info("Errors Found in read_from_cluster ~w",
                        [length(ErrorsFound)]),
            length(ErrorsFound);
        _ ->
            ?assertEqual(Errors, length(ErrorsFound))
    end.

find_tombs(Node, Bucket, KR, MR, ResultType) ->
    lager:info("Finding tombstones from node ~p.", [Node]),
    {ok, C} = riak:client_connect(Node),
    case ResultType of
        return_keys ->
            riak_client:aae_fold({find_tombs, Bucket, KR, all, MR}, C);
        return_count ->
            riak_client:aae_fold({reap_tombs, Bucket, KR, all, MR, count}, C)
    end.

find_keys(Node, Bucket, KR, MR) ->
    lager:info("Finding keys from node ~p.", [Node]),
    {ok, C} = riak:client_connect(Node),
    {ok, KL} =
        riak_client:aae_fold(
            {find_keys, Bucket, KR, MR, {sibling_count, 0}},
            C),
    length(KL).