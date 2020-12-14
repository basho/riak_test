%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_sibling).
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

-define(SNK_WORKERS, 4).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).
-define(COMMMON_VAL_SIB, <<"CommonValueToWriteForAllSiblingObjects">>).
-define(COMMMON_VAL_FIN, <<"CommonValueToWriteForAllFinalObjects">>).

-define(REPL_SLEEP, 4096). 
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
            {replrtq_srcqueue, SrcQueueDefns}
          ]}
        ]).

-define(SNK_CONFIG(ClusterName, PeerList), 
        [{riak_kv, 
            [{replrtq_enablesink, true},
                {replrtq_sinkqueue, ClusterName},
                {replrtq_sinkpeers, PeerList},
                {replrtq_sinkworkers, ?SNK_WORKERS}]}]).

confirm() ->
    ClusterASrcQ = "cluster_b:any",
    ClusterBSrcQ = "cluster_c:any",
    ClusterCSrcQ = "cluster_a:any",

    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, ClusterASrcQ)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, ClusterBSrcQ)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, ClusterCSrcQ)}]),
    
    lager:info("Discover Peer IP/ports and restart with peer config"),
    FoldToPeerConfig = 
        fun(Node, Acc) ->
            {http, {IP, Port}} =
                lists:keyfind(http, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":http"
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterC),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA),
    ClusterCSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB),
    ClusterASNkCfg = ?SNK_CONFIG(cluster_a, ClusterASnkPL),
    ClusterBSNkCfg = ?SNK_CONFIG(cluster_b, ClusterBSnkPL),
    ClusterCSNkCfg = ?SNK_CONFIG(cluster_c, ClusterCSnkPL),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterASNkCfg) end,
                    ClusterA),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterBSNkCfg) end,
                    ClusterB),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterCSNkCfg) end,
                    ClusterC),           

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB ++ ClusterC),
    
    lager:info("Ready for test."),
    test_repl_between_clusters(ClusterA, ClusterB, ClusterC).

test_repl_between_clusters(ClusterA, ClusterB, ClusterC) ->

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

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
    % Write keys to cluster A, verify B does have these changes
    write_to_cluster(NodeA, 1, 1000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1, 1000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, 0),
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL},
                            cluster_b),
    
    lager:info("Test replicating tombstones"),
    delete_from_cluster(NodeA, 901, 1000),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeB, 901, 1000, ?COMMMON_VAL_INIT, 100),
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL},
                            cluster_b),
    
    lager:info("Confirm that cluster C has no objects"),
    lager:info("Chaining real-time replication doesn't replicate"),
    lager:info("Real-time repl requires a mesh topology between clusters"),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, 1000),

    lager:info("Rounds of full-sync should replicate all, no more than 12"),
    FSCFun = 
        fun() ->
            R = fullsync_check({NodeB, IPB, PortB, ?B_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL},
                                cluster_c),
            R == {root_compare, 0}
        end,
    ok = rt:wait_until(FSCFun, 12, ?REPL_SLEEP div 4),
    read_from_cluster(NodeC, 1, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),


    lager:info("Create some siblings - if we update in A then in C"),
    write_to_cluster(NodeA, 1, 100, ?COMMMON_VAL_MOD),
    write_to_cluster(NodeC, 1, 100, ?COMMMON_VAL_SIB),
    timer:sleep(?REPL_SLEEP + ?REPL_SLEEP), % double sleep as no local read
    lager:info("A should have siblings"),
    lager:info("B and C should have different versions but not siblings"),
    read_from_cluster(NodeB, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_from_cluster(NodeC, 1, 100, ?COMMMON_VAL_SIB, 0),
    read_sibsfrom_cluster(NodeA, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),
    lager:info("All other objects undisturbed"),
    read_from_cluster(NodeA, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeB, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeB, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeC, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),

    lager:info("Full sync from B to C should create siblings in C"),
    {clock_compare, 100}
        = fullsync_check({NodeB, IPB, PortB, ?B_NVAL},
                            {NodeC, IPC, PortC, ?C_NVAL},
                            cluster_c),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_sibsfrom_cluster(NodeC, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),
    read_sibsfrom_cluster(NodeA, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),

    lager:info("Full sync from A to B should create siblings in B"),
    {clock_compare, 100}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL},
                            cluster_b),
    timer:sleep(?REPL_SLEEP + ?REPL_SLEEP), % double sleep as no local read
    read_sibsfrom_cluster(NodeB, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),
    read_sibsfrom_cluster(NodeC, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),
    read_sibsfrom_cluster(NodeA, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),

    lager:info("All other objects undistrubed"),
    read_from_cluster(NodeA, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeB, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeB, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeC, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),

    lager:info("Replace sibling on Node A"),
    write_to_cluster(NodeA, 1, 100, ?COMMMON_VAL_FIN),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1, 100, ?COMMMON_VAL_FIN, 0),
    read_from_cluster(NodeB, 1, 100, ?COMMMON_VAL_FIN, 0),
    read_sibsfrom_cluster(NodeC, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),
    
    lager:info("Full sync from C to A will find deltas but no repairs"),
    {clock_compare, 100}
        = fullsync_check({NodeC, IPC, PortC, ?C_NVAL},
                            {NodeA, IPA, PortA, ?A_NVAL},
                            cluster_a),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1, 100, ?COMMMON_VAL_FIN, 0),
    read_from_cluster(NodeB, 1, 100, ?COMMMON_VAL_FIN, 0),
    read_sibsfrom_cluster(NodeC, 1, 100, [?COMMMON_VAL_MOD, ?COMMMON_VAL_SIB]),

    lager:info("Full sync from B to C should re-align all"),
    {clock_compare, 100}
        = fullsync_check({NodeB, IPB, PortB, ?B_NVAL},
                            {NodeC, IPC, PortC, ?C_NVAL},
                            cluster_c),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1, 100, ?COMMMON_VAL_FIN, 0),
    read_from_cluster(NodeB, 1, 100, ?COMMMON_VAL_FIN, 0),
    read_from_cluster(NodeC, 1, 100, ?COMMMON_VAL_FIN, 0),
    read_from_cluster(NodeA, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeB, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeB, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeC, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),


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
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]),

    % lager:info("Sleeping to await queue drain."),
    % timer:sleep(2000),
    
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


read_sibsfrom_cluster(Node, Start, End, Values) ->
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            ExpectedVals = 
                lists:sort(
                    lists:map(fun(ValBin) ->
                                    <<N:32/integer, ValBin/binary>>
                                end,
                                Values)),
            case  riak_client:get(?TEST_BUCKET, Key, C) of
                {ok, Obj} ->
                    case lists:sort(riak_object:get_values(Obj)) of
                        ExpectedVals ->
                            Acc;
                        UnexpectedVal ->
                            [{wrong_value, Key, UnexpectedVal}|Acc]
                    end;
                {error, Error} ->
                    [{fetch_error, Error, Key}|Acc]
            end
        end,
    ErrorsFound = lists:foldl(F, [], lists:seq(Start, End)),
    ?assertEqual(0, length(ErrorsFound)).