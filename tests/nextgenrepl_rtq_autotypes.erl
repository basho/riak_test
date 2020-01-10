%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_rtq_autotypes).
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

-define(REPL_SLEEP, 512). 
    % May need to wait for 2 x the 256ms max sleep time of a snk worker
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
           {delete_mode, keep},
           {enable_repl_cache, true},
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
    ClusterASrcQ = "cluster_b:buckettype.type1|cluster_c:bucketprefix._",
    ClusterBSrcQ = "cluster_a:any|cluster_c:bucketprefix._",
    ClusterCSrcQ = "cluster_b:buckettype.type1|cluster_a:any",

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
    rt:join_cluster(ClusterC),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB ++ ClusterC),
    

    lager:info("Play around with sink worker counts"),
    [NodeA|_RestA] = ClusterA,
    not_found =
        rpc:call(NodeA,
                    riak_kv_replrtq_snk,
                    set_workercount,
                    [cluster_b, ?SNK_WORKERS + 1]),
    ok =
        rpc:call(NodeA,
                    riak_kv_replrtq_snk,
                    set_workercount,
                    [cluster_a, ?SNK_WORKERS + 1]),
    ok =
        rpc:call(NodeA,
                    riak_kv_replrtq_snk,
                    set_workercount,
                    [cluster_a, ?SNK_WORKERS]),

    lager:info("Creating bucket types 'type1' and 'type2'"),
    rt:create_and_activate_bucket_type(hd(ClusterA),
                                        <<"type1">>, [{magic, false}]),
    rt:create_and_activate_bucket_type(hd(ClusterB),
                                        <<"type1">>, [{magic, false}]),
    rt:create_and_activate_bucket_type(hd(ClusterC),
                                        <<"type1">>, [{magic, false}]),
    
    rt:create_and_activate_bucket_type(hd(ClusterA),
                                        <<"type2">>, [{magic, true}]),
    rt:create_and_activate_bucket_type(hd(ClusterB),
                                        <<"type2">>, [{magic, true}]),
    rt:create_and_activate_bucket_type(hd(ClusterC),
                                        <<"type2">>, [{magic, true}]),

    lager:info("Ready for test."),
    test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC).

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
    write_to_cluster(NodeA, 1, 1000, new_obj, {<<"type1">>, <<"b1">>}),
    write_to_cluster(NodeA, 1, 1000, new_obj, {<<"type1">>, <<"_b1">>}),
    write_to_cluster(NodeA, 1, 1000, new_obj, {<<"type2">>, <<"_b1">>}),
    write_to_cluster(NodeA, 1, 1000, new_obj, <<"_b1">>),
    write_to_cluster(NodeA, 1, 1000, new_obj, <<"b2">>),
    timer:sleep(?REPL_SLEEP),
    lager:info("Confirm expected replication"),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, {<<"type1">>, <<"b1">>}, 0),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, {<<"type1">>, <<"_b1">>}, 0),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, {<<"type1">>, <<"_b1">>}, 0),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, {<<"type2">>, <<"_b1">>}, 0),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, <<"_b1">>, 0),

    lager:info("Confirm non-matching configuration not replicated"),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, {<<"type2">>, <<"_b1">>}, 1000),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, <<"_b1">>, 1000),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, <<"b2">>, 1000),

    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, {<<"type1">>, <<"b1">>}, 1000),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, <<"b2">>, 1000),
    
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
write_to_cluster(Node, Start, End, CommonValBin, Bucket) ->
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
                        riak_object:new(Bucket,
                                        Key,
                                        <<N:32/integer, CVB/binary>>);
                    UpdateBin ->
                        UPDV = <<N:32/integer, UpdateBin/binary>>,
                        {ok, PrevObj} = C:get(Bucket, Key),
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

% delete_from_cluster(Node, Start, End, Bucket) ->
%     lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
%     lager:warning("Note that only utf-8 keys are used"),
%     {ok, C} = riak:client_connect(Node),
%     F = 
%         fun(N, Acc) ->
%             Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
%             try C:delete(Bucket, Key) of
%                 ok ->
%                     Acc;
%                 Other ->
%                     [{N, Other} | Acc]
%             catch
%                 What:Why ->
%                     [{N, {What, Why}} | Acc]
%             end
%         end,
%     Errors = lists:foldl(F, [], lists:seq(Start, End)),
%     lager:warning("~p errors while deleting: ~p", [length(Errors), Errors]),
%     ?assertEqual([], Errors).


%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, CommonValBin, Bucket, Errors) ->
    read_from_cluster(Node, Start, End, CommonValBin, Bucket, Errors, false).

read_from_cluster(Node, Start, End, CommonValBin, Bucket, Errors, LogErrors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            case  C:get(Bucket, Key) of
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
            ?assertEqual(Errors, length(ErrorsFound))
    end.
