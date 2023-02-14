%% @doc
%% This module implements a riak_test to prove real-time repl
%% works as expected with automated discovery of peers

-module(nextgenrepl_bouncingtomb).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 32).
-define(C_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 3).
-define(C_NVAL, 1).

-define(SNK_WORKERS, 12).
-define(PEER_LIMIT, 6).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).

-define(INIT_MAX_DELAY, 10).
-define(STND_MAX_DELAY, 3600).
-define(BIG_REPL_SLEEP, 4000).

-define(LOOP_COUNT, 5).
-define(STATS_WAIT, 1000).


-define(CORE_CONFIG(RingSize, NVal),[
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
        }]).

-define(AAE_CONFIG,
        [
            {anti_entropy, {off, []}},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
            {tictacaae_storeheads, true},
                % Necessary to count tombstones using reap_tombs query
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600},
            {tictacaae_exchangetick, 3600000}, % don't tick for an hour!
            {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
        ]).

-define(CONFIG(RingSize, NVal, Q, DeleteMode), 
        ?CORE_CONFIG(RingSize, NVal) ++
        [{riak_kv,
            ?AAE_CONFIG ++
            [
                {ttaaefs_maxresults, 1024},
                {ttaaefs_queuename, Q},
                {replrtq_enablesrc, true},
                {replrtq_srcqueue, atom_to_list(Q) ++ ":any"},
                {delete_mode, DeleteMode}
            ]
        }]).

-define(SNK_CONFIG(RingSize, Nval, Q, ClusterName, IP, P, DeleteMode), 
        ?CORE_CONFIG(RingSize, Nval) ++
        [{riak_kv,
            ?AAE_CONFIG ++
            [
                {ttaaefs_maxresults, 1024},
                {ttaaefs_queuename, Q},
                {replrtq_enablesrc, true},
                {replrtq_srcqueue, atom_to_list(Q) ++ ":any"},
                {delete_mode, DeleteMode},
                {replrtq_enablesink, true},
                {replrtq_prompt_max_seconds, ?INIT_MAX_DELAY},
                {replrtq_sinkqueue, ClusterName},
                {replrtq_sinkpeers, IP ++ ":" ++ integer_to_list(P) ++ ":pb"},
                {replrtq_sinkworkers, ?SNK_WORKERS},
                {replrtq_sinkpeerlimit, ?PEER_LIMIT},
                {replrtq_peer_discovery, true},
                {ttaaefs_scope, all},
                {ttaaefs_peerip, IP},
                {ttaaefs_peerport, P},
                {ttaaefs_peerprotocol, pb},
                {ttaaefs_allcheck, 0},
                {ttaaefs_nocheck, 24}
            ]}
        ]).

confirm() ->

    lager:info("***************"),
    lager:info("Testing rotating tombs with key insomnia"),
    lager:info("***************"),
    
    [ClusterA1, ClusterB1] =
        rt:deploy_clusters([
            {3, ?CONFIG(?A_RING, ?A_NVAL, cluster_b, 1000)},
            {3, ?CONFIG(?B_RING, ?B_NVAL, cluster_a, keep)}]),

    pass = with_insomnia_test(ClusterA1, ClusterB1),
    
    rt:clean_cluster(ClusterA1),
    rt:clean_cluster(ClusterB1),

    lager:info("***************"),
    lager:info("Testing rotating tombs without key insomnia"),
    lager:info("***************"),

    [ClusterA2, ClusterB2, ClusterC2] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, cluster_b, keep)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, cluster_a, 1000)},
            {1, ?CONFIG(?C_RING, ?C_NVAL, q1_ttaaefs, keep)}]),
    
    no_insomnia_test(ClusterA2, ClusterB2, ClusterC2).

no_insomnia_test(ClusterA, ClusterB, ClusterC) ->

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),

    [NodeA1, NodeA2] = ClusterA,
    [NodeB1, NodeB2] = ClusterB,
    [NodeC1] = ClusterC,

    PeerConfigFun =
        fun(Node) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            {IP, Port}
        end,
    
    reset_peer_config(NodeA1, cluster_a, ?A_RING, ?A_NVAL, cluster_b, PeerConfigFun(NodeB1), 1000),
    reset_peer_config(NodeA2, cluster_a, ?A_RING, ?A_NVAL, cluster_b, PeerConfigFun(NodeB2), 1000),
    reset_peer_config(NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA1), keep),
    reset_peer_config(NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA2), keep),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lager:info("Confirm riak_kv is up on all nodes."),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),
    
    lager:info("Wait for peer discovery"),
    timer:sleep((?INIT_MAX_DELAY + 1) * 1000),

    lager:info("Ready for test - writing keys"),
    write_to_cluster(NodeC1, 1, 1000, new_obj),

    lager:info("Manual push of keys to A and B"),
    {IPc, Portc} = PeerConfigFun(NodeC1),
    {ok, SrcC} = riakc_pb_socket:start(IPc, Portc),
    {ok, SnkA} = riak:client_connect(NodeA1),
    {ok, SnkB} = riak:client_connect(NodeB1),
    ?assertMatch(1000, drain_queue(SrcC, SnkA)),

    lager:info("Ready for test - deleting keys"),
    delete_from_cluster(NodeC1, 1, 1000),

    ?assertMatch(1000, drain_queue(SrcC, SnkB)),
    rt:wait_until(count_keys_fun(NodeB1, 1000, reap_tombs)),

    riakc_pb_socket:stop(SrcC),

    lager:info("Resetting delete_mode on Cluster B"),
    reset_peer_config(NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA1), 1000),
    reset_peer_config(NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA2), 1000),

    GetStatsFun =
        fun() ->
            lager:info("Cluster A stats"),
            get_stats(NodeA1),
            get_stats(NodeA2),
            lager:info("Cluster B stats"),
            get_stats(NodeB1),
            get_stats(NodeB2)
        end,
    LogFun =
        fun(NodeA, NodeB) ->
            {ok, A1C} =
                rpc:call(NodeA,
                    riak_client,
                    aae_fold,
                    [{reap_tombs, ?TEST_BUCKET, all, all, all, count}]),
            lager:info("Cluster A ~w tombs", [A1C]),

            {ok, B1C} =
                rpc:call(NodeB,
                    riak_client,
                    aae_fold,
                    [{reap_tombs, ?TEST_BUCKET, all, all, all, count}]),
            lager:info("Cluster B ~w tombs", [B1C]),

            {A1C, B1C}
        end,
    
    lager:info("Cluster B has tombstones, but Cluster A has objects"),
    {_InitACount, _InitBCount} = LogFun(NodeA1, NodeB1),
    
    GetStatsFun(),

    rotating_full_sync(NodeA1, NodeB1, GetStatsFun, LogFun, ?LOOP_COUNT),

    ?assertMatch({0, 0}, LogFun(NodeA1, NodeB1)),

    pass.


with_insomnia_test(ClusterA, ClusterB) ->

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),

    [NodeA1, NodeA2, NodeA3] = ClusterA,
    [NodeB1, NodeB2, NodeB3] = ClusterB,

    PeerConfigFun =
        fun(Node) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            {IP, Port}
        end,
    
    reset_peer_config(NodeA1, cluster_a, ?A_RING, ?A_NVAL, cluster_b, PeerConfigFun(NodeB1), 1000),
    reset_peer_config(NodeA2, cluster_a, ?A_RING, ?A_NVAL, cluster_b, PeerConfigFun(NodeB2), 1000),
    reset_peer_config(NodeA3, cluster_a, ?A_RING, ?A_NVAL, cluster_b, PeerConfigFun(NodeB3), 1000),
    reset_peer_config(NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA1), keep),
    reset_peer_config(NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA2), keep),
    reset_peer_config(NodeB3, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA3), keep),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lager:info("Confirm riak_kv is up on all nodes."),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),

    lager:info("Wait for peer discovery"),
    timer:sleep((?INIT_MAX_DELAY + 1) * 1000),

    lager:info("Ready for test"),
    write_to_cluster(NodeA1, 1, 1000, new_obj),
    rt:wait_until(count_keys_fun(NodeA1, 1000, erase_keys)),
    rt:wait_until(count_keys_fun(NodeB1, 1000, erase_keys)),
    lager:info("Deleting objects from other cluster"),
    delete_from_cluster(NodeB1, 1, 1000),
    rt:wait_until(count_keys_fun(NodeB1, 1000, reap_tombs)),
    rt:wait_until(count_keys_fun(NodeA1, 0, reap_tombs)),
    rt:wait_until(count_keys_fun(NodeB1, 1000, reap_tombs)),

    lager:info("Switching delete mode from keep on Cluster B"),
    reset_peer_config(NodeB1, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA1), 1000),
    reset_peer_config(NodeB2, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA2), 1000),
    reset_peer_config(NodeB3, cluster_b, ?B_RING, ?B_NVAL, cluster_a,  PeerConfigFun(NodeA3), 1000),

    GetStatsFun =
        fun() ->
            lager:info("Cluster A stats"),
            get_stats(NodeA1),
            get_stats(NodeA2),
            get_stats(NodeA3),
            lager:info("Cluster B stats"),
            get_stats(NodeB1),
            get_stats(NodeB2),
            get_stats(NodeB3)
        end,
    LogFun =
        fun(NodeA, NodeB) ->
            {ok, A1C} =
                rpc:call(NodeA,
                    riak_client,
                    aae_fold,
                    [{reap_tombs, ?TEST_BUCKET, all, all, all, count}]),
            lager:info("Cluster A ~w tombs", [A1C]),

            {ok, B1C} =
                rpc:call(NodeB,
                    riak_client,
                    aae_fold,
                    [{reap_tombs, ?TEST_BUCKET, all, all, all, count}]),
            lager:info("Cluster B ~w tombs", [B1C]),

            {A1C, B1C}
        end,

    lager:info("Cluster B has tombstones, but Cluster A should have reaped"),
    {_InitACount, _InitBCount} = LogFun(NodeA1, NodeB1),
    
    GetStatsFun(),

    rotating_full_sync(NodeA1, NodeB1, GetStatsFun, LogFun, ?LOOP_COUNT),

    ?assertMatch({0, 0}, LogFun(NodeA1, NodeB1)),

    pass.

rotating_full_sync(NodeA, NodeB, GetStatsFun, LogFun, 0) ->
    LoopLogFun = 
        fun(X) ->
            lager:info("Closing count loop ~w", [X]),
            _ = LogFun(NodeA, NodeB)
        end,
    GetStatsFun(),
    lists:foreach(LoopLogFun, lists:seq(1, ?LOOP_COUNT)),
    GetStatsFun();
rotating_full_sync(NodeA, NodeB, GetStatsFun, LogFun, Rotations) ->
    lager:info("Full sync from Cluster B - loops to go ~w", [Rotations]),
    rpc:call(NodeB, riak_client, ttaaefs_fullsync, [all_check, 60]),
    timer:sleep(?BIG_REPL_SLEEP),
    _ = LogFun(NodeA, NodeB),
    lager:info("Full sync from Cluster A - loops to go ~w", [Rotations]),
    rpc:call(NodeA, riak_client, ttaaefs_fullsync, [all_check, 60]),
    timer:sleep(?BIG_REPL_SLEEP),
    _ = LogFun(NodeA, NodeB),
    GetStatsFun(),
    rotating_full_sync(NodeA, NodeB, GetStatsFun, LogFun, Rotations - 1).

reset_peer_config(Node, ClusterName, RS, NV, Q, PeerX, DeleteMode) ->
    {IP, Port} = PeerX,
    ClusterSNkCfg = ?SNK_CONFIG(RS, NV, Q, ClusterName, IP, Port, DeleteMode),
    rt:set_advanced_conf(Node, ClusterSNkCfg).

count_keys_fun(N, ExpectedC, Type) ->
    fun() ->
        {ok, C} =
            rpc:call(N,
                riak_client,
                aae_fold,
                [{Type, ?TEST_BUCKET, all, all, all, count}]),
        lager:info("Count of ~w on ~w for ~w", [C, N, Type]),
        ExpectedC == C
    end.

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
                        {ok, PrevObj} = riak_client:get(?TEST_BUCKET, Key, C),
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

delete_from_cluster(Node, Start, End) ->
    lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            try riak_client:delete(?TEST_BUCKET, Key, C) of
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


drain_queue(SrcC, SnkA) ->
    drain_queue(SrcC, SnkA, 0).

drain_queue(SrcC, SnkA, N) ->
    case riakc_pb_socket:fetch(SrcC, <<"q1_ttaaefs">>) of
        {ok, queue_empty} ->
            N;
        {ok, {deleted, _TombClock, RObj}} ->
            {ok, _LMD} = riak_client:push(RObj, true, [], SnkA),
            drain_queue(SrcC, SnkA, N + 1);
        {ok, RObj} ->
            {ok, _LMD} = riak_client:push(RObj, false, [], SnkA),
            drain_queue(SrcC, SnkA, N + 1)
    end.

get_stats(Node) ->
    S = verify_riak_stats:get_stats(Node, ?STATS_WAIT),
    {<<"ngrfetch_prefetch_total">>, PFT} =
        lists:keyfind(<<"ngrfetch_prefetch_total">>, 1, S),
    {<<"ngrfetch_tofetch_total">>, TFT} =
        lists:keyfind(<<"ngrfetch_tofetch_total">>, 1, S),
    {<<"ngrfetch_nofetch_total">>, NFT} =
        lists:keyfind(<<"ngrfetch_nofetch_total">>, 1, S),
    {<<"ngrrepl_object_total">>, FOT} =
        lists:keyfind(<<"ngrrepl_object_total">>, 1, S),
    {<<"ngrrepl_error_total">>, FErT} =
        lists:keyfind(<<"ngrrepl_error_total">>, 1, S),
    {<<"ngrrepl_empty_total">>, FEmT} =
        lists:keyfind(<<"ngrrepl_empty_total">>, 1, S),
    {<<"read_repairs_total">>, RRT} =
        lists:keyfind(<<"read_repairs_total">>, 1, S),
    lager:info(
        "Stats for Node ~w, PFT=~w TFT=~w NFT=~w FOT=~w FErT=~w FEmT=~w RRT=~w",
        [Node, PFT, TFT, NFT, FOT, FErT, FEmT, RRT]).