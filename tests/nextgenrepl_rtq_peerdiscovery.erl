%% @doc
%% This module implements a riak_test to prove real-time repl
%% works as expected with automated discovery of peers

-module(nextgenrepl_rtq_peerdiscovery).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 32).
-define(C_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 2).
-define(C_NVAL, 3).

-define(SNK_WORKERS, 8).
-define(PEER_LIMIT, 2).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(INIT_MAX_DELAY, 60).
-define(STND_MAX_DELAY, 3600).

-define(REPL_SLEEP, 2048). 
    % May need to wait for 2 x the 1024ms max sleep time of a snk worker
-define(WAIT_LOOPS, 12).

-define(CONFIG(RingSize, NVal, SrcQueueDefns), [
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
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600},
            {tictacaae_exchangetick, 120 * 1000},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {ttaaefs_maxresults, 128},
            {delete_mode, keep},
            {replrtq_enablesrc, true},
            {replrtq_srcqueue, SrcQueueDefns},
            {replrtq_peer_discovery, true}
          ]}
        ]).

-define(SNK_CONFIG(ClusterName, PeerList), 
        [{riak_kv, 
            [{replrtq_enablesink, true},
                {replrtq_prompt_max_seconds, ?INIT_MAX_DELAY},
                {replrtq_sinkqueue, ClusterName},
                {replrtq_sinkpeers, PeerList},
                {replrtq_sinkworkers, ?SNK_WORKERS},
                {replrtq_sinkpeerlimit, ?PEER_LIMIT}]}]).

confirm() ->
    ClusterASrcQ = "cluster_b:any|cluster_c:any",
    ClusterBSrcQ = "cluster_a:any|cluster_c:any",
    ClusterCSrcQ = "cluster_b:any|cluster_a:any",

    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {1, ?CONFIG(?A_RING, ?A_NVAL, ClusterASrcQ)},
            {3, ?CONFIG(?B_RING, ?B_NVAL, ClusterBSrcQ)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, ClusterCSrcQ)}]),
    
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),

    lager:info("Discover Peer IP/ports for pb and restart with peer config"),
    lists:foreach(compare_peer_info(1), ClusterA),
    lists:foreach(compare_peer_info(3), ClusterB),
    lists:foreach(compare_peer_info(2), ClusterC),

    [NodeA|_RestA] = ClusterA,
    [NodeB|_RestB] = ClusterB,
    [NodeC|_RestC] = ClusterC,

    PeerConfigFun =
        fun(Node) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            IP ++ ":" ++ integer_to_list(Port) ++ ":" ++ "pb"
        end,
    
    PeerA = PeerConfigFun(NodeA),
    PeerB = PeerConfigFun(NodeB),
    PeerC = PeerConfigFun(NodeC),

    reset_peer_config(ClusterA, cluster_a, PeerB, PeerC),
    reset_peer_config(ClusterB, cluster_b, PeerA, PeerC),
    reset_peer_config(ClusterC, cluster_c, PeerA, PeerB),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lager:info("Confirm riak_kv is up on all nodes."),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB ++ ClusterC),

    lager:info("Ready for test - with protocol buffers client for rtq."),
    pass =
        test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC),
    StatsA1 = get_stats(ClusterA),
    StatsB1 = get_stats(ClusterB),
    lager:info("ClusterA stats ~w", [StatsA1]),
    lager:info("ClusterB stats ~w", [StatsB1]),

    pass.

compare_peer_info(ExpectedPeers) ->
    fun(Node) ->
        {pb, {IP, Port}} =
            lists:keyfind(pb, 1, rt:connection_info(Node)),
        MemberList = rpc:call(Node, riak_client, membership_request, [pb]),
        lager:info("Discovered Member list ~p", [MemberList]),
        ?assert(lists:member({IP, Port}, MemberList)),
        ?assertMatch(ExpectedPeers, length(MemberList))
    end.

reset_peer_config(SnkCluster, ClusterName, PeerX, PeerY) ->
    ClusterSNkCfg = ?SNK_CONFIG(ClusterName, PeerX ++ "|" ++ PeerY),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterSNkCfg) end,
                    SnkCluster).

current_peers(Node, QueueName) ->
    rpc:call(Node, riak_kv_replrtq_snk, current_peers, [QueueName]).

worker_counts(Node) ->
    rpc:call(Node, riak_kv_replrtq_snk, get_worker_counts, []).

reset_cluster_worker_counts(Node, WorkerCount, PerPeerLimit) ->
    rpc:call(Node, riak_client, replrtq_reset_all_workercounts, [WorkerCount, PerPeerLimit]).

reset_cluster_peers(Node, QueueName) ->
    rpc:call(Node, riak_client, replrtq_reset_all_peers, [QueueName]).    

test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC) ->

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

    lager:info("Sleep for peer discovery"),
    timer:sleep((?INIT_MAX_DELAY + 1) * 1000),
    set_max_delay(ClusterA ++ ClusterB ++ ClusterC, ?STND_MAX_DELAY),

    lager:info("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    {http, {IPC, PortC}} = lists:keyfind(http, 1, rt:connection_info(NodeC)),
    lager:info("Cluster A ~s ~w Cluster B ~s ~w Cluster C ~s ~w",
                [IPA, PortA, IPB, PortB, IPC, PortC]),
    
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC}),

    lager:info("Test 1000 key difference and resolve"),
    % Write keys to cluster A, verify B and C do have them.
    write_to_cluster(NodeA, 1, 1000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, 0),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC}),
    
    lager:info("Test replicating tombstones"),
    delete_from_cluster(NodeA, 901, 1000),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeB, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC}),

    lager:info("Test replicating modified objects"),
    write_to_cluster(NodeB, 1, 100, ?COMMMON_VAL_MOD),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_from_cluster(NodeC, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_from_cluster(NodeA, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC}),
    
    lager:info("Test replicating from Cluster C"),
    lager:info("Test 1000 key difference and resolve"),
    % Write keys to cluster A, verify B and C do have them.
    write_to_cluster(NodeC, 1001, 2000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1001, 2000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeB, 1001, 2000, ?COMMMON_VAL_INIT, 0),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC}),

    lager:info("Check peers stable"),
    ?assertNot(check_peers_stable(NodeA, cluster_a)),
    ?assertNot(check_peers_stable(NodeB, cluster_b)),
    ?assertNot(check_peers_stable(NodeC, cluster_c)),

    lager:info("Node A current peers ~p", [current_peers(NodeA, cluster_a)]),
    lager:info("Node B current peers ~p", [current_peers(NodeB, cluster_b)]),
    lager:info("Node C current peers ~p", [current_peers(NodeC, cluster_c)]),
    ?assertMatch({8, 2}, worker_counts(NodeA)),
    ?assertMatch({8, 2}, worker_counts(NodeB)),
    ?assertMatch({8, 2}, worker_counts(NodeC)),

    lager:info("Confirm no peers change on cluster-wide reset"),
    ?assertMatch([], reset_cluster_peers(NodeA, cluster_a)),
    ?assertMatch([], reset_cluster_peers(NodeB, cluster_b)),
    ?assertMatch([], reset_cluster_peers(NodeC, cluster_c)),

    lager:info("Confirm all peers change on cluster-wide count reset"),
    ?assertMatch(1, length(reset_cluster_worker_counts(NodeA, 12, 3))),
    ?assertMatch(3, length(reset_cluster_worker_counts(NodeB, 12, 3))),
    ?assertMatch(2, length(reset_cluster_worker_counts(NodeC, 12, 3))),

    lager:info("Test replicating from Cluster B"),
    lager:info("Test 1000 key difference and resolve"),
    % Write keys to cluster B, verify A and C do have them.
    write_to_cluster(NodeB, 2001, 3000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 2001, 3000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 2001, 3000, ?COMMMON_VAL_INIT, 0),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC}),

    lager:info("Confirm modified worker counts"),
    lists:foreach(
        fun(N) -> ?assertMatch({12, 3}, worker_counts(N)) end,
        ClusterA ++ ClusterB ++ ClusterC),

    pass.


check_all_insync({NodeA, IPA, PortA},
                    {NodeB, IPB, PortB},
                    {NodeC, IPC, PortC}) ->
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL},
                            cluster_a),
    {root_compare, 0}
        = fullsync_check({NodeB, IPB, PortB, ?B_NVAL},
                            {NodeC, IPC, PortC, ?C_NVAL},
                            cluster_c),
    {root_compare, 0}
        = fullsync_check({NodeC, IPC, PortC, ?C_NVAL},
                            {NodeA, IPA, PortA, ?A_NVAL},
                            cluster_a),
    true.

fullsync_check({SrcNode, _SrcIP, _SrcPort, SrcNVal},
                {_SinkNode, SinkIP, SinkPort, SinkNVal},
                SnkClusterName) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [SnkClusterName]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]).

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


%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, CommonValBin, Errors) ->
    read_from_cluster(Node, Start, End, CommonValBin, Errors, false).

read_from_cluster(Node, Start, End, CommonValBin, Errors, LogErrors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            case  riak_client:get(?TEST_BUCKET, Key, C) of
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
                        [length(ErrorsFound)]);
        _ ->
            case LogErrors of
                true ->
                    LogFun = 
                        fun(Error) ->
                            lager:info("Read error ~w", [Error])
                        end,
                    lists:foreach(LogFun, ErrorsFound);
                false ->
                    ok
            end,
            % case length(ErrorsFound) of
            %     Errors ->
            %         ok;
            %     _ ->
            %         lists:foreach(fun(E) -> lager:warning("Read error ~w", [E]) end, ErrorsFound)
            % end,
            ?assertEqual(Errors, length(ErrorsFound))
    end.


get_stats(Cluster) ->
    Stats = {0, 0, 0, 0, 0, 0},
        % {prefetch, tofetch, nofetch, object, error, empty}
    lists:foldl(fun(N, {PFAcc, TFAcc, NFAcc, FOAcc, FErAcc, FEmAcc}) -> 
                        S = verify_riak_stats:get_stats(N),
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
                        {PFT + PFAcc, TFT + TFAcc, NFT + NFAcc,
                            FOT + FOAcc, FErT + FErAcc, FEmAcc + FEmT}
                    end,
                    Stats,
                    Cluster).

set_max_delay([], S) ->
    lager:info("Set Max Delay on all clusters to ~w", [S]);
set_max_delay([Node|Rest], S) ->
    rpc:call(
        Node,
        application,
        set_env,
        [riak_kv, replrtq_prompt_max_seconds, S]),
    set_max_delay(Rest, S).

check_peers_stable(Node, QueueName) ->
    rpc:call(Node, riak_kv_replrtq_peer, update_discovery, [QueueName]).