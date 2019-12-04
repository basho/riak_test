%% @doc
%% What happens when we run AAE full-sync between clusters with different
%% delete_modes.  The answer is problematic - tombstones differ from nothing
%% (as that is the point of a tombstone), and so we can't expect two clusters
%% that have had the same operations to agree.

-module(nextgenrepl_deletemodes).
-behavior(riak_test).
-export([confirm/0]).
-export([read_from_cluster/5, length_find_tombs/3]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(C_RING, 16).
-define(A_NVAL, 1).
-define(B_NVAL, 3).
-define(C_NVAL, 2).

-define(KEY_COUNT, 10000).
-define(REPL_SLEEP, 10000).
-define(LOOP_COUNT, 10).

-define(SNK_WORKERS, 4).

-define(DELETE_DELAY, 10000).

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
            {tictacaae_exchangetick, 120 * 1000},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {delete_mode, DeleteMode}
          ]}
        ]).

-define(REPL_CONFIG(LocalClusterName, PeerList, SrcQueueDefns), [
    {riak_kv,
        [
            {replrtq_srcqueue, SrcQueueDefns},
            {replrtq_enablesink, true},
            {replrtq_sinkqueue, LocalClusterName},
            {replrtq_sinkpeers, PeerList},
            {replrtq_sinkworkers, ?SNK_WORKERS}
        ]}
]).


repl_config(RemoteCluster1, RemoteCluster2, LocalClusterName, PeerList) ->
    ?REPL_CONFIG(LocalClusterName,
                    PeerList,
                    atom_to_list(RemoteCluster1) ++ ":any|"
                        ++ atom_to_list(RemoteCluster2) ++ ":any").


confirm() ->
    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, keep)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, immediate)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, ?DELETE_DELAY)}]),

    lager:info("Test run using PB protocol an a mix of delete modes"),
    test_repl(pb, [ClusterA, ClusterB, ClusterC]),
    
    pass.


test_repl(Protocol, [ClusterA, ClusterB, ClusterC]) ->

    [NodeA1, NodeA2] = ClusterA,
    [NodeB1, NodeB2] = ClusterB,
    [NodeC1, NodeC2] = ClusterC,

    FoldToPeerConfig = 
        fun(Node, Acc) ->
            {Protocol, {IP, Port}} =
                lists:keyfind(Protocol, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port)
                ++ ":" ++ atom_to_list(Protocol)
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB ++ ClusterC),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterC),
    ClusterCSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterB),

    ACfg = repl_config(cluster_b, cluster_c, cluster_a, ClusterASnkPL),
    BCfg = repl_config(cluster_a, cluster_c, cluster_b, ClusterBSnkPL),
    CCfg = repl_config(cluster_b, cluster_a, cluster_c, ClusterCSnkPL),
    rt:set_advanced_conf(NodeA1, ACfg),
    rt:set_advanced_conf(NodeA2, ACfg),
    rt:set_advanced_conf(NodeB1, BCfg),
    rt:set_advanced_conf(NodeB2, BCfg),
    rt:set_advanced_conf(NodeC1, CCfg),
    rt:set_advanced_conf(NodeC2, CCfg),

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB ++ ClusterC),
    
    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),
    
    {Protocol, {NodeB1ip, NodeB1port}} =
        lists:keyfind(Protocol, 1, rt:connection_info(NodeB1)),
    {Protocol, {NodeC1ip, NodeC1port}} =
        lists:keyfind(Protocol, 1, rt:connection_info(NodeC1)),
    lager:info("Following deletes, and waiting for delay - B and C equal"),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    lager:info("A should differ from B/C as tombstones not empty"),
    {clock_compare, Delta1} = 
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_b},
                        {NodeB1ip, NodeB1port, ?B_NVAL}),
    {clock_compare, Delta2} = 
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    lager:info("Now that tombstones have been re-replicated - B and C differ"),
    {clock_compare, Delta3} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    lager:info("Delta A to B ~w A to C ~w and B to C ~w",
                [Delta1, Delta2, Delta3]),
    
    lager:info("Find all tombstones in cluster A"),
    {ok, BKdhL} = find_tombs(NodeA1, all, all),
    ?assertMatch(?KEY_COUNT, length(BKdhL)),
    
    reap_from_cluster(NodeA1, 1, ?KEY_COUNT),
    timer:sleep(?DELETE_DELAY +  1000),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    lager:info("As tombstones reaped A, B and C the same"),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_b},
                        {NodeB1ip, NodeB1port, ?B_NVAL}),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    
    lager:info("Confirm no tombstones in any cluster"),
    {ok, BKdhLA} = find_tombs(NodeA1, all, all),
    ?assertMatch(0, length(BKdhLA)),
    {ok, BKdhLB} = find_tombs(NodeB1, all, all),
    ?assertMatch(0, length(BKdhLB)),
    {ok, BKdhLC} = find_tombs(NodeC1, all, all),
    ?assertMatch(0, length(BKdhLC)),

    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),
    lager:info("Find all tombstones in cluster A"),
    {ok, BKdhL1} = find_tombs(NodeA1, all, all),
    ?assertMatch(?KEY_COUNT, length(BKdhL1)),
    reap_from_cluster(NodeA1, BKdhL1),
    timer:sleep(?DELETE_DELAY +  1000),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    lager:info("As tombstones reaped A, B and C the same"),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_b},
                        {NodeB1ip, NodeB1port, ?B_NVAL}),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    lager:info("Confirm no tombstones in any cluster"),
    {ok, BKdhLA} = find_tombs(NodeA1, all, all),
    ?assertMatch(0, length(BKdhLA)),
    {ok, BKdhLB} = find_tombs(NodeB1, all, all),
    ?assertMatch(0, length(BKdhLB)),
    {ok, BKdhLC} = find_tombs(NodeC1, all, all),
    ?assertMatch(0, length(BKdhLC)),

    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),
    reap_from_cluster(NodeA1, {job, 1}),
    lager:info("Immediate reap count ~w after fsm managed reap",
                [length_find_tombs(NodeA1, all, all)]),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeA1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeB1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeC1, all, all], 0, 20),

    write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2),
    reap_from_cluster(NodeA1, local),
    lager:info("Immediate reap count ~w after distributed reap",
                [length_find_tombs(NodeA1, all, all)]),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeA1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeB1, all, all], 0, 20),
    wait_for_outcome(?MODULE, length_find_tombs, [NodeC1, all, all], 0, 20),

    {root_compare, 0} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    lager:info("As tombstones reaped A, B and C the same"),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_b},
                        {NodeB1ip, NodeB1port, ?B_NVAL}),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeA1, ?A_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),
    {root_compare, 0} =
        fullsync_check(Protocol, {NodeB1, ?B_NVAL, cluster_c},
                        {NodeC1ip, NodeC1port, ?C_NVAL}),

    pass.

fullsync_check(Protocol, {SrcNode, SrcNVal, SnkCluster},
                {SinkIP, SinkPort, SinkNVal}) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [SnkCluster]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [Protocol, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]),
    AAEResult.


%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, CommonValBin) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj = 
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMMON_VAL_INIT,
                        riak_object:new(?TEST_BUCKET,
                                        Key,
                                        <<N:32/integer, CVB/binary>>);
                    UpdateBin ->
                        UPDV = <<N:32/integer, UpdateBin/binary>>,
                        {ok, PrevObj} = C:get(?TEST_BUCKET, Key),
                        riak_object:update_value(PrevObj, UPDV)
                end,
            try C:put(Obj) of
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

delete_from_cluster(Node, Start, End) ->
    lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            try C:delete(?TEST_BUCKET, Key) of
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

reap_from_cluster(Node, Start, End) ->
    lager:info("Reaping ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            try riak_client:reap(?TEST_BUCKET, Key, C) of
                true ->
                    Acc;
                false ->
                    [{N, false} | Acc];
                Other ->
                    [{N, Other} | Acc]
            catch
                What:Why ->
                    [{N, {What, Why}} | Acc]
            end
        end,
    Aborts = lists:foldl(F, [], lists:seq(Start, End)),
    lager:warning("~p aborts while reaping: ~p", [length(Aborts), Aborts]),
    ?assertEqual([], Aborts).

reap_from_cluster(Node, BKdhL) when is_list(BKdhL) ->
    lager:info("Reaping ~p found tombs from node ~p.", [length(BKdhL), Node]),
    {ok, C} = riak:client_connect(Node),
    F =
        fun({B, K, DH}, Acc) ->
            try riak_client:reap(B, K, DH, C) of
                true ->
                    Acc;
                false ->
                    [{K, false} | Acc];
                Other ->
                    [{K, Other} | Acc]
            catch
                What:Why ->
                    [{K, {What, Why}} | Acc]
            end
        end,
    Aborts = lists:foldl(F, [], BKdhL),
    lager:warning("~p aborts while reaping: ~p", [length(Aborts), Aborts]),
    ?assertEqual([], Aborts);
reap_from_cluster(Node, Job) ->
    lager:info("Auto-reaping found tombs from node ~p Job ~p", [Node, Job]),
    {ok, C} = riak:client_connect(Node),
    Query = {reap_tombs, ?TEST_BUCKET, all, all, all, Job},
    {ok, Count} = riak_client:aae_fold(Query, C),
    ?assertEqual(?KEY_COUNT, Count).
    

read_from_cluster(Node, Start, End, CommonValBin, Errors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            case  C:get(?TEST_BUCKET, Key) of
                {ok, Obj} ->
                    ExpectedVal = <<N:32/integer, CommonValBin/binary>>,
                    case riak_object:get_value(Obj) of
                        ExpectedVal ->
                            Acc;
                        UnexpectedVal ->
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

length_find_tombs(Node, KR, MR) ->
    {ok, L} = find_tombs(Node, KR, MR),
    length(L).

find_tombs(Node, KR, MR) ->
    lager:info("Finding tombstones from node ~p.", [Node]),
    {ok, C} = riak:client_connect(Node),
    riak_client:aae_fold({find_tombs, ?TEST_BUCKET, KR, all, MR}, C).


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

write_then_delete(NodeA1, NodeA2, NodeB1, NodeB2, NodeC1, NodeC2) ->
    lager:info("Write ~w objects into A and read from B and C", [?KEY_COUNT]),
    write_to_cluster(NodeA1, 1, ?KEY_COUNT, new_obj),
    timer:sleep(max(?DELETE_DELAY, ?REPL_SLEEP)),
    0 = 
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeB1, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    0 = 
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeC1, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    
    lager:info("Deleting ~w objects from B and read not_found from A and C", [?KEY_COUNT]),
    delete_from_cluster(NodeB2, 1, ?KEY_COUNT),
    timer:sleep(max(?DELETE_DELAY, ?REPL_SLEEP)),
    ?KEY_COUNT =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeA2, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                        ?KEY_COUNT,
                        ?LOOP_COUNT),
    ?KEY_COUNT =
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeC2, 1, ?KEY_COUNT, ?COMMMON_VAL_INIT, undefined],
                            ?KEY_COUNT,
                            ?LOOP_COUNT).