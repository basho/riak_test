%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_rtq_auto).
-behavior(riak_test).
-export([confirm/0]).
-export([fullsync_check/3]).
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

-define(REPL_SLEEP, 2048). 
    % May need to wait for 2 x the 1024ms max sleep time of a snk worker
-define(WAIT_LOOPS, 12).

-define(CONFIG(RingSize, NVal, ObjL, SrcQueueDefns), [
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
            {delete_mode, keep},
            {replrtq_enablesrc, true},
            {replrtq_srcobjectlimit, ObjL},
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
    ClusterASrcQ = "cluster_b:any|cluster_c:any",
    ClusterBSrcQ = "cluster_a:any|cluster_c:any",
    ClusterCSrcQ = "cluster_b:any|cluster_a:any",

    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, 100, ClusterASrcQ)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, 0, ClusterBSrcQ)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, 10, ClusterCSrcQ)}]),
    
    lager:info("Discover Peer IP/ports and restart with peer config"),
    FoldToPeerConfigHTTP = 
        fun(Node, Acc) ->
            {http, {IP, Port}} =
                lists:keyfind(http, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":http"
        end,
    reset_peer_config(FoldToPeerConfigHTTP, ClusterA, ClusterB, ClusterC),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB ++ ClusterC),
    
    lager:info("Ready for test - with http client for rtq."),
    test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC),
    
    lager:info("Discover Peer IP/ports for pb and restart with peer config"),
    FoldToPeerConfigPB = 
        fun(Node, Acc) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":" ++ "pb"
        end,

    rt:clean_cluster(ClusterA),
    rt:clean_cluster(ClusterB),
    rt:clean_cluster(ClusterC),

    [ClusterApb, ClusterBpb, ClusterCpb] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, 0, ClusterASrcQ)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, 10, ClusterBSrcQ)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, 100, ClusterCSrcQ)}]),
    reset_peer_config(FoldToPeerConfigPB, ClusterApb, ClusterBpb, ClusterCpb),
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterApb),
    rt:wait_until_ring_converged(ClusterBpb),
    rt:wait_until_ring_converged(ClusterCpb),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterApb ++ ClusterBpb ++ ClusterCpb),
    
    lager:info("Ready for test - with protocol buffers client for rtq."),
    test_rtqrepl_between_clusters(ClusterApb, ClusterBpb, ClusterCpb),
    pass.


reset_peer_config(FoldToPeerConfig, ClusterA, ClusterB, ClusterC) ->
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB ++ ClusterC),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterC),
    ClusterCSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA ++ ClusterB),
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
    rt:join_cluster(ClusterC).


test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC) ->

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
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]),

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
