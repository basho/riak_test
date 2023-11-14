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

-module(nextgenrepl_reaptombs).
-behavior(riak_test).
-export([confirm/0]).
-export([read_from_cluster/6]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 16).
-define(A_NVAL, 2).
-define(B_NVAL, 3).

-define(KEY_COUNT, 20000).

-define(LOOP_COUNT, 10).
-define(TOMB_PAUSE, 2).

-define(SNK_WORKERS, 4).

-define(DELETE_WAIT, 8000).
%% This must be increased, otherwise tombstones may be reaped before their
%% presence can be checked in the test


-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(CONFIG(RingSize, NVal, DeleteMode), [
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
            {delete_mode, DeleteMode}
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
    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {3, ?CONFIG(?A_RING, ?A_NVAL, keep)},
            {3, ?CONFIG(?B_RING, ?B_NVAL, keep)}]),

    lager:info("***************************************************"),
    lager:info("Test run using PB protocol on a healthy cluster"),
    lager:info("***************************************************"),
    pass = test_repl_reap(pb, [ClusterA, ClusterB]),

    lager:info("***************************************************"),
    lager:info("Test run using PB protocol with node failure"),
    lager:info("***************************************************"),
    pass = test_repl_reap_with_node_down(ClusterA, ClusterB),

    rt:clean_cluster(ClusterA),
    rt:clean_cluster(ClusterB),

    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {3, ?CONFIG(?A_RING, ?A_NVAL, keep)},
            {3, ?CONFIG(?B_RING, ?B_NVAL, keep)}]),

    lager:info("***************************************************"),
    lager:info("Test run using HTTP protocol on a healthy cluster"),
    lager:info("***************************************************"),
    pass = test_repl_reap(http, [ClusterA, ClusterB]),
    
    pass.


test_repl_reap(Protocol, [ClusterA, ClusterB]) ->

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
    
    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, ?TEST_BUCKET),
    
    {Protocol, {NodeA1ip, NodeA1port}} =
        lists:keyfind(Protocol, 1, rt:connection_info(NodeA1)),
    lager:info("Following deletes, and waiting for delay - A and B equal"),
    root_compare(
        Protocol,
        {NodeB1, ?B_NVAL, cluster_a},
        {NodeA1ip, NodeA1port, ?A_NVAL}),

    lager:info("Confirm key count of tombs in both clusters"),
    {ok, TCA1} = find_tombs(NodeA1, ?TEST_BUCKET, all, all, return_count),
    {ok, TCB1} = find_tombs(NodeB1, ?TEST_BUCKET, all, all, return_count),
    ?assertEqual(?KEY_COUNT, TCA1),
    ?assertEqual(?KEY_COUNT, TCB1),

    reap_from_cluster(NodeA1, local, ?TEST_BUCKET),
    lager:info("Confirm all keys reaped from both clusters"),
    rt:wait_until(
        fun() ->
            {ok, 0} == find_tombs(NodeA1, ?TEST_BUCKET, all, all, return_count)
        end),
    lager:info("All reaped from Cluster A"),
    lager:info("Now would expect ClusterB to quickly be in sync"),
    lager:info("So waiting only 5 seconds"),
    rt:wait_until(
        fun() ->
            {ok, 0} == find_tombs(NodeB1, ?TEST_BUCKET, all, all, return_count)
        end,
        5,
        1000
    ),

    lager:info("Setup bucket type for test on both Clusters"),
    Type = <<"mytype">>,
    TypeProps = [{n_val, 1}],
    lager:info("Create bucket type ~p, wait for propagation", [Type]),
    rt:create_and_activate_bucket_type(NodeA1, Type, TypeProps),
    rt:wait_until_bucket_type_status(Type, active, ClusterA),
    rt:wait_until_bucket_props(ClusterA, {Type, <<"bucket">>}, TypeProps),
    rt:create_and_activate_bucket_type(NodeB1, Type, TypeProps),
    rt:wait_until_bucket_type_status(Type, active, ClusterB),
    rt:wait_until_bucket_props(ClusterB, {Type, <<"bucket">>}, TypeProps),

    lager:info("Load keys into typed bucket"),
    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, {Type, ?TEST_BUCKET}),

    lager:info("Confirm key count of tombs in both clusters"),
    {ok, TCA2} =
        find_tombs(NodeA1, {Type, ?TEST_BUCKET}, all, all, return_count),
    {ok, TCB2} =
        find_tombs(NodeB1, {Type, ?TEST_BUCKET}, all, all, return_count),
    ?assertEqual(?KEY_COUNT, TCA2),
    ?assertEqual(?KEY_COUNT, TCB2),

    reap_from_cluster(NodeA1, local, {Type, ?TEST_BUCKET}),
    lager:info("Confirm all keys reaped from both clusters"),
    rt:wait_until(
        fun() ->
            {ok, 0} ==
                find_tombs(
                    NodeA1, {Type, ?TEST_BUCKET}, all, all, return_count)
        end),
    lager:info("All reaped from Cluster A"),
    lager:info("Now would expect ClusterB to quickly be in sync"),
    lager:info("So waiting only 5 seconds"),
    rt:wait_until(
        fun() ->
            {ok, 0} ==
                find_tombs(
                    NodeB1, {Type, ?TEST_BUCKET}, all, all, return_count)
        end,
        5,
        1000
    ),

    lager:info(
        "Confirm reaps are not looping around - all reaper queues empty"),
    ReapQueueFun =
        fun(N) ->
            {mqueue_lengths, MQLs} =
                lists:keyfind(
                    mqueue_lengths,
                    1,
                    rpc:call(N, riak_kv_reaper, reap_stats, [])),
            lager:info("Reap queue lengths ~w on ~w", [MQLs, N]),
            QS = lists:sum(lists:map(fun({_P, L}) -> L end, MQLs)),
            ?assert(QS == 0)
        end,
    lists:foreach(ReapQueueFun, ClusterA  ++ ClusterB),

    pass.


test_repl_reap_with_node_down(ClusterA, ClusterB) ->

    [NodeA1, NodeA2, _NodeA3] = ClusterA,
    [NodeB1, NodeB2, _NodeB3] = ClusterB,

    lager:info("Test again - but with failure in A"),
    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, ?TEST_BUCKET),
    lager:info("Confirm key count of tombs in both clusters"),
    {ok, TCA1} = find_tombs(NodeA1, ?TEST_BUCKET, all, all, return_count),
    {ok, TCB1} = find_tombs(NodeB1, ?TEST_BUCKET, all, all, return_count),
    ?assertEqual(?KEY_COUNT, TCA1),
    ?assertEqual(?KEY_COUNT, TCB1),

    lager:info("Stopping node 2 in A"),
    rt:stop_and_wait(NodeA2),

    lager:info("Fold to trigger reap of all tombs - whilst node down"),
    reap_from_cluster(NodeA1, local, ?TEST_BUCKET),

    rt:start_and_wait(NodeA2),
    lists:foreach(fun rt:wait_until_node_handoffs_complete/1, ClusterA),
    lager:info("Node 2 restarted"),


    lager:info("Confirm all keys reaped from both clusters"),
    rt:wait_until(
        fun() ->
            {ok, 0} == find_tombs(NodeA1, ?TEST_BUCKET, all, all, return_count)
        end),
    lager:info("All reaped from Cluster A"),
    lager:info("Now would expect ClusterB to quickly be in sync"),
    lager:info("So waiting only 5 seconds"),
    rt:wait_until(
        fun() ->
            {ok, 0} == find_tombs(NodeB1, ?TEST_BUCKET, all, all, return_count)
        end,
        5,
        1000
    ),

    lager:info("Test again - but with failure in B"),
    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, ?TEST_BUCKET),
    lager:info("Confirm key count of tombs in both clusters"),
    {ok, TCA1} = find_tombs(NodeA1, ?TEST_BUCKET, all, all, return_count),
    {ok, TCB1} = find_tombs(NodeB1, ?TEST_BUCKET, all, all, return_count),
    ?assertEqual(?KEY_COUNT, TCA1),
    ?assertEqual(?KEY_COUNT, TCB1),

    lager:info("Stopping node 2 in B"),
    rt:stop_and_wait(NodeB2),

    lager:info("Fold to trigger reap of all tombs - whilst node down"),
    reap_from_cluster(NodeA1, local, ?TEST_BUCKET),

    rt:start_and_wait(NodeB2),
    lists:foreach(fun rt:wait_until_node_handoffs_complete/1, ClusterB),
    lager:info("Node 2 restarted"),

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

    pass.

fullsync_check(Protocol, {SrcNode, SrcNVal, SnkCluster},
                {SinkIP, SinkPort, SinkNVal}) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [SnkCluster]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [Protocol, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]),
    AAEResult.


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


wait_for_outcome(Module, Func, Args, ExpOutcome, Loops) ->
    wait_for_outcome(Module, Func, Args, ExpOutcome, 0, Loops).

wait_for_outcome(Module, Func, Args, _ExpOutcome, LoopCount, LoopCount) ->
    apply(Module, Func, Args);
wait_for_outcome(Module, Func, Args, ExpOutcome, LoopCount, MaxLoops) ->
    case apply(Module, Func, Args) of
        ExpOutcome ->
            ExpOutcome;
        NotRightYet ->
            lager:info("~w not yet ~w ~w", [Func, ExpOutcome, NotRightYet]),
            timer:sleep(LoopCount * 2000),
            wait_for_outcome(Module, Func, Args, ExpOutcome,
                                LoopCount + 1, MaxLoops)
    end.

write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, Bucket) ->
    lager:info("Write ~w objects into A and read from B", [?KEY_COUNT]),
    write_to_cluster(NodeA1, Bucket, 1, ?KEY_COUNT, new_obj),
    lager:info("Waiting to read sample"),
    0 = 
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeB1, Bucket, ?KEY_COUNT - 31, ?KEY_COUNT,
                                ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    lager:info("Waiting to read all"),
    0 = 
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeB1, Bucket, 1, ?KEY_COUNT,
                                ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    
    lager:info("Deleting ~w objects from B and read not_found from A", [?KEY_COUNT]),
    delete_from_cluster(NodeB2, Bucket, 1, ?KEY_COUNT),
    lager:info("Waiting for missing sample"),
    32 =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeA2, Bucket, ?KEY_COUNT - 31, ?KEY_COUNT,
                            ?COMMMON_VAL_INIT, undefined],
                        32,
                        ?LOOP_COUNT),
    lager:info("Waiting for all missing"),
    ?KEY_COUNT =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeA2, Bucket, 1, ?KEY_COUNT,
                            ?COMMMON_VAL_INIT, undefined],
                        ?KEY_COUNT,
                        ?LOOP_COUNT),
    lager:info("Write and delete cycle confirmed").


root_compare(
    Protocol,
    {NodeX, XNVAL, QueueName},
    {NodeY, YPort, YNVAL}) ->
    timer:sleep(?DELETE_WAIT),
    R =
        fullsync_check(
            Protocol,
            {NodeX, XNVAL, QueueName},
            {NodeY, YPort, YNVAL}),
    {root_compare, 0} =
        case R of
            {Outcome, N} when N < 10, Outcome =/= root_compare ->
                %% There is a problem here with immediate mode delete
                %% in that it can sometimes fail to clean up the odd
                %% tombstone.
                %% It was for this reason the tombstone_delay was added
                %% but amending this cannot stop an intermittent issue
                %% Workaround for the purpose of this test is to permit
                %% a small discrepancy in this case
                lager:warning(
                    "Immediate delete issue - ~w not cleared ~w",
                    [N, Outcome]),
                timer:sleep(2 * ?DELETE_WAIT),
                root_compare(
                    Protocol,
                    {NodeX, XNVAL, QueueName},
                    {NodeY, YPort, YNVAL});
            R ->
                R
        end.