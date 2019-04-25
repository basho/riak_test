%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_rtq_stdobj).
-behavior(riak_test).
-export([confirm/0]).
-export([test_rtqrepl_between_clusters/4]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(C_RING, 16).
-define(A_NVAL, 1).
-define(B_NVAL, 3).
-define(C_NVAL, 2).

-define(SNK_WORKERS, 4).

-define(CONFIG(RingSize, NVal), [
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
           {delete_mode, keep}
          ]}
        ]).

confirm() ->
    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL)},
            {2, ?CONFIG(?B_RING, ?B_NVAL)},
            {2, ?CONFIG(?C_RING, ?C_NVAL)}]),
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
    test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC,
                                    fun fullsync_check/2).

test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC, FullSyncFun) ->
    
    setup_srcreplqueues(ClusterA, [cluster_b, cluster_c], any),
    setup_srcreplqueues(ClusterB, [cluster_a, cluster_c], any),
    setup_srcreplqueues(ClusterC, [cluster_a, cluster_b], any),
    setup_snkreplworkers(ClusterB, ClusterA, cluster_a),
    setup_snkreplworkers(ClusterC, ClusterA, cluster_a),
    setup_snkreplworkers(ClusterA, ClusterB, cluster_b),
    setup_snkreplworkers(ClusterC, ClusterB, cluster_b),
    setup_snkreplworkers(ClusterA, ClusterC, cluster_c),
    setup_snkreplworkers(ClusterB, ClusterC, cluster_c),

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

    lager:info("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    {http, {IPC, PortC}} = lists:keyfind(http, 1, rt:connection_info(NodeC)),
    lager:info("Cluster A ~s ~w Cluster B ~s ~w Cluster C ~s ~w",
                [IPA, PortA, IPB, PortB, IPC, PortC]),
    
    {root_compare, 0}
        = FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0}
        = FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {root_compare, 0}
        = FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),

    lager:info("Test 1000 key difference and resolve"),
    % Write keys to cluster A, verify B and C do have them.
    write_to_cluster(NodeA, 1, 1000),
    read_from_cluster(NodeB, 1, 1000, 0),
    read_from_cluster(NodeC, 1, 1000, 0),
    {root_compare, 0}
        = FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0}
        = FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {root_compare, 0}
        = FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),
    
    lager:info("Test replicating tombstones"),
    delete_from_cluster(NodeA, 901, 1000),
    read_from_cluster(NodeA, 901, 1000, 100),
    read_from_cluster(NodeB, 901, 1000, 100),
    read_from_cluster(NodeC, 901, 1000, 100),

    {root_compare, 0}
        = FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0}
        = FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {root_compare, 0}
        = FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),

    pass.



setup_srcreplqueues([], _SinkClusters, _Filter) ->
    ok;
setup_srcreplqueues([SrcNode|Rest], SinkClusters, Filter) ->
    SetupQueueFun =
        fun(ClusterName) ->
            true = rpc:call(SrcNode,
                            riak_kv_replrtq_src,
                            register_rtq,
                            [ClusterName, Filter])
        end,
    lists:foreach(SetupQueueFun, SinkClusters),
    setup_srcreplqueues(Rest, SinkClusters, Filter).


setup_snkreplworkers([], _SnkNodes, _SnkName) ->
    ok;
setup_snkreplworkers([{http, {IP, Port}}|Rest], SnkNodes, SnkName) ->
    SetupSnkFun = 
        fun(Node) ->
            ok = rpc:call(Node,
                            riak_kv_replrtq_snk,
                            add_snkqueue,
                            [SnkName, [{1, 0, IP, Port}], ?SNK_WORKERS])
        end,
    lists:foreach(SetupSnkFun, SnkNodes),
    setup_snkreplworkers(Rest, SnkNodes, SnkName);
setup_snkreplworkers(SrcCluster, SnkNodes, SnkName) ->
    ConnMapFun = 
        fun(Node) ->
            lists:keyfind(http, 1, rt:connection_info(Node))
        end,
    setup_snkreplworkers(lists:map(ConnMapFun, SrcCluster),
                            SnkNodes,
                            SnkName).



fullsync_check({SrcNode, SrcIP, SrcPort, SrcNVal},
                {_SinkNode, SinkIP, SinkPort, SinkNVal}) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, [ModRef]),
    ok = rpc:call(SrcNode, ModRef, set_source, [ModRef, http, SrcIP, SrcPort]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [ModRef, http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [ModRef, SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]),

    % lager:info("Sleeping to await queue drain."),
    % timer:sleep(2000),
    
    AAEResult.

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj = riak_object:new(?TEST_BUCKET,
                                    Key,
                                    <<N:32/integer, CommonValBin/binary>>),
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
read_from_cluster(Node, Start, End, Errors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
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
                            [{wrong_value, UnexpectedVal}|Acc]
                    end;
                {error, Error} ->
                    [{fetch_error, Error}|Acc]
            end
        end,
    ErrorsFound = lists:foldl(F, [], lists:seq(Start, End)),
    case Errors of
        undefined ->
            lager:info("Errors Found in read_from_cluster ~w",
                        [length(ErrorsFound)]);
        _ ->
            ?assertEqual(Errors, length(ErrorsFound))
    end.
