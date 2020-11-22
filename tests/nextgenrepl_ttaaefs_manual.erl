%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_ttaaefs_manual).
-behavior(riak_test).
-export([confirm/0]).
-export([test_repl_between_clusters/5,
            write_to_cluster/6,
            read_from_cluster/6,
            delete_from_cluster/4,
            key/1]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(C_RING, 16).
-define(A_NVAL, 1).
-define(B_NVAL, 3).
-define(C_NVAL, 2).

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
           {ttaaefs_maxresults, 128},
           {ttaaefs_rangeboost, 1}, % So maxresults is consistent
           {delete_mode, keep}
          ]}
        ]).

confirm() ->
    [ClusterA1, ClusterB1, ClusterC1] = setup_clusters(),
    pass = test_repl_between_clusters(ClusterA1, ClusterB1, ClusterC1,
                                        rangesync_checkfun(),
                                        none,
                                        fun setup_replqueues/1),
    rt:clean_cluster(ClusterA1),
    rt:clean_cluster(ClusterB1),
    rt:clean_cluster(ClusterC1),
    
    [ClusterA2, ClusterB2, ClusterC2] = setup_clusters(),
    test_repl_between_clusters(ClusterA2, ClusterB2, ClusterC2,
                                        fun fullsync_check/2,
                                        fun partialsync_check/4,
                                        fun setup_replqueues/1).

setup_clusters() ->
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
    [ClusterA, ClusterB, ClusterC].

test_repl_between_clusters(ClusterA, ClusterB, ClusterC,
                            FullSyncFun, SetupReplFun) ->
    test_repl_between_clusters(ClusterA, ClusterB, ClusterC,
                            FullSyncFun, none, SetupReplFun).

test_repl_between_clusters(ClusterA, ClusterB, ClusterC,
                            FullSyncFun, PartialSyncFun, SetupReplFun) ->
    
    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),
    
    ok = SetupReplFun(ClusterA ++ ClusterB ++ ClusterC),

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

    lager:info("Test 100 key difference and resolve"),
    % Write keys to cluster A, verify B and C do not have them.
    write_to_cluster(NodeA, 1, 100),
    read_from_cluster(NodeB, 1, 100, 100),
    read_from_cluster(NodeC, 1, 100, 100),
    {clock_compare, 100}
        = FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    {clock_compare, 100}
        = FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    % Now node 3 should align with node 1 
    {root_compare, 0}
        = FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),
    read_from_cluster(NodeA, 1, 100, 0),
    read_from_cluster(NodeB, 1, 100, 0),
    read_from_cluster(NodeC, 1, 100, 0),
    {root_compare, 0}
        = FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0}
        = FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),

    lager:info("Test 1000 key difference and resolve"),
    write_to_cluster(NodeA, 101, 1100),
    read_from_cluster(NodeB, 101, 1100, 1000),
    read_from_cluster(NodeC, 101, 1100, 1000),

    {clock_compare, N1}
        = FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    lager:info("First comparison found ~w differences", [N1]),
    ?assertEqual(true, N1 > 100),
    ?assertEqual(true, N1 < 1000),

    lager:info("Further eight loops should complete repair"),
    LoopRepairFun =
        fun(SrcInfo, SnkInfo) ->
            fun(_I) ->
                FullSyncFun(SrcInfo, SnkInfo)
            end
        end,
    lists:foreach(LoopRepairFun({NodeA, IPA, PortA, ?A_NVAL},
                                    {NodeB, IPB, PortB, ?B_NVAL}),
                    lists:seq(1, 8)),
    {root_compare, 0} =
        FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                    {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0} =
        FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                    {NodeA, IPA, PortA, ?A_NVAL}),
    lager:info("NodeA and NodeB has been re-sync'd"),
    read_from_cluster(NodeB, 1, 1100, 0),

    lager:info("Repairing in the wrong direction doesn't repair"),
    {clock_compare, 128} =
        FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                    {NodeB, IPB, PortB, ?B_NVAL}),
    read_from_cluster(NodeC, 101, 1100, 1000),

    lager:info("Complete repair from different clusters"),
    lists:foreach(LoopRepairFun({NodeA, IPA, PortA, ?A_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL}),
                    lists:seq(1, 5)),
    lager:info("... reading after repairs from node A"),
    read_from_cluster(NodeC, 1, 1100, undefined),
    lists:foreach(LoopRepairFun({NodeB, IPB, PortB, ?B_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL}),
                    lists:seq(1, 5)),
    lager:info("... reading after repairs from node B"),
    read_from_cluster(NodeC, 1, 1100, undefined),
    read_from_cluster(NodeC, 1, 1100, 0),

    lager:info("Test replicating tombstones"),
    delete_from_cluster(NodeA, 901, 1000),
    read_from_cluster(NodeA, 901, 1000, 100),
    lager:info("Confirm that replicating back doesn't remove tombstones ..."),
    lager:info("... But it will see differences"),
    {clock_compare, 100} =
        FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),
    read_from_cluster(NodeA, 901, 1000, 100),
    lager:info("Confirm that replicating forward does migrate tombstones"),
    {clock_compare, 100} =
        FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    read_from_cluster(NodeB, 901, 1000, 100),
    read_from_cluster(NodeC, 901, 1000, 0),
    {clock_compare, 100} =
        FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    read_from_cluster(NodeC, 901, 1000, 100),

    {root_compare, 0} =
        FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0} =
        FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {root_compare, 0} =
        FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),
    


    case PartialSyncFun of
        none ->
            pass;
        _ ->
            lager:info("Compare the bucket - dynamic AAE not based on cached trees"),
            Now = os:timestamp(),
            timer:sleep(1000),
            {tree_compare, 0} =
                PartialSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL},
                                hour_sync,
                                Now),
            {tree_compare, 0} =
                PartialSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL},
                                hour_sync,
                                Now),
            {tree_compare, 0} =
                PartialSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                                {NodeA, IPA, PortA, ?A_NVAL},
                                hour_sync,
                                Now),
            
            lager:info("Test further 1000 key differences"),
            write_to_cluster(NodeA, 1101, 2100),
            {clock_compare, N2}
                = PartialSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                    {NodeB, IPB, PortB, ?B_NVAL},
                                    hour_sync,
                                    os:timestamp()),
            lager:info("First comparison found ~w differences", [N2]),
            ?assertEqual(true, N2 > 100),
            ?assertEqual(true, N2 < 1000),
            LoopPartialRepairFun =
                fun(SrcInfo, SnkInfo, TS) ->
                    fun(_I) ->
                        PartialSyncFun(SrcInfo, SnkInfo, hour_sync, TS)
                    end
                end,
            lists:foreach(LoopPartialRepairFun({NodeA, IPA, PortA, ?A_NVAL},
                                                {NodeB, IPB, PortB, ?B_NVAL},
                                                os:timestamp()),
                    lists:seq(1, 8)),
            {tree_compare, 0} =
                PartialSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL},
                                hour_sync,
                                os:timestamp()),
            lager:info("Differences A -> B resolved"),

            lager:info("Wind now back to before changes - and don't see differences"),
            
            {tree_compare, 0} =
                PartialSyncFun({NodeA, IPA, PortA, ?B_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL},
                                hour_sync,
                                Now),
            lager:info("Look from now - and see the differences"),
            {clock_compare, N3}
                = PartialSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                    {NodeC, IPC, PortC, ?C_NVAL},
                                    hour_sync,
                                    os:timestamp()),
            {clock_compare, N4}
                = PartialSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                    {NodeC, IPC, PortC, ?C_NVAL},
                                    day_sync,
                                    os:timestamp()),
            {clock_compare, N5}
                = PartialSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                    {NodeC, IPC, PortC, ?C_NVAL},
                                    all_sync,
                                    os:timestamp()),
            ?assertEqual(true, N3 > 100),
            ?assertEqual(true, N3 < 1000),
            ?assertEqual(true, N4 > 100),
            ?assertEqual(true, N4 < 1000),
            ?assertEqual(true, N5 > 100),
            ?assertEqual(true, N5 < 1000),
            lager:info("Five more syncs should fix it"),
            lists:foreach(LoopPartialRepairFun({NodeA, IPA, PortA, ?A_NVAL},
                                                {NodeC, IPC, PortC, ?C_NVAL},
                                                os:timestamp()),
                    lists:seq(1, 5)),

            lager:info("Loop around syncing to confirm all OK"),
            {root_compare, 0} =
                FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}),
            {root_compare, 0} =
                FullSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL}),
            {root_compare, 0} =
                FullSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}),
            {root_compare, 0} =
                FullSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                                {NodeA, IPA, PortA, ?A_NVAL}),
            
            {tree_compare, 0} =
                PartialSyncFun({NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL},
                                day_sync,
                                os:timestamp()),
            {tree_compare, 0} =
                PartialSyncFun({NodeB, IPB, PortB, ?B_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL},
                                day_sync,
                                os:timestamp()),
            {tree_compare, 0} =
                PartialSyncFun({NodeC, IPC, PortC, ?C_NVAL},
                                {NodeA, IPA, PortA, ?A_NVAL},
                                day_sync,
                                os:timestamp()),
            pass
    end.


setup_replqueues([]) ->
    ok;
setup_replqueues([HeadNode|Others]) ->
    false = rpc:call(HeadNode,
                    riak_kv_replrtq_src,
                    register_rtq,
                    [q1_ttaaefs, block_rtq]),
        % false indicates this queue is already defined by default
    setup_replqueues(Others).

fullsync_check({SrcNode, SrcIP, SrcPort, SrcNVal},
                {SinkNode, SinkIP, SinkPort, SinkNVal}) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]),
    SrcHTTPC = rhc:create(SrcIP, SrcPort, "riak", []),
    {ok, SnkC} = riak:client_connect(SinkNode),
    N = drain_queue(SrcHTTPC, SnkC),
    lager:info("Drained queue and pushed ~w objects", [N]),
    AAEResult.

rangesync_checkfun() ->
    Start = calendar:now_to_datetime(os:timestamp()),
    timer:sleep(1000),
    fun({SrcNode, SrcIP, SrcPort, SrcNVal},
            {SinkNode, SinkIP, SinkPort, SinkNVal}) ->
        ModRef = riak_kv_ttaaefs_manager,
        _ = rpc:call(SrcNode, ModRef, pause, []),
        ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
        ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
        Now = calendar:now_to_datetime(os:timestamp()),
        ok = rpc:call(SrcNode, ModRef, set_range, [?TEST_BUCKET, all, Start, Now]),
        AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [range_sync, 60]),
        SrcHTTPC = rhc:create(SrcIP, SrcPort, "riak", []),
        {ok, SnkC} = riak:client_connect(SinkNode),
        N = drain_queue(SrcHTTPC, SnkC),
        lager:info("Drained queue and pushed ~w objects", [N]),
        AAEResult
    end.

partialsync_check({SrcNode, SrcIP, SrcPort, _SrcNVal},
                    {SinkNode, SinkIP, SinkPort, _SinkNVal},
                    SyncRange,
                    Now) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_bucketsync, [[?TEST_BUCKET]]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [SyncRange, 60, Now]),
    SrcHTTPC = rhc:create(SrcIP, SrcPort, "riak", []),
    {ok, SnkC} = riak:client_connect(SinkNode),
    N = drain_queue(SrcHTTPC, SnkC),
    lager:info("Drained queue and pushed ~w objects", [N]),
    AAEResult.

drain_queue(SrcClient, SnkClient) ->
    drain_queue(SrcClient, SnkClient, 0).

drain_queue(SrcClient, SnkClient, N) ->
    case rhc:fetch(SrcClient, q1_ttaaefs) of
        {ok, queue_empty} ->
            N;
        {ok, {deleted, _TombClock, RObj}} ->
            {ok, _LMD} = riak_client:push(RObj, true, [], SnkClient),
            drain_queue(SrcClient, SnkClient, N + 1);
        {ok, RObj} ->
            {ok, _LMD} = riak_client:push(RObj, false, [], SnkClient),
            drain_queue(SrcClient, SnkClient, N + 1)
    end.


%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End) ->
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    write_to_cluster(Node, Start, End, ?TEST_BUCKET, true, CommonValBin).

write_to_cluster(Node, Start, End, Bucket, NewObj, CVB) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = key(N),
            Obj = 
                case NewObj of
                    true ->
                        riak_object:new(Bucket,
                                        Key,
                                        <<N:32/integer, CVB/binary>>);
                    false ->
                        UPDV = <<N:32/integer, CVB/binary>>,
                        {ok, PrevObj} = riak_client:get(Bucket, Key, C),
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
    delete_from_cluster(Node, Start, End, ?TEST_BUCKET).

delete_from_cluster(Node, Start, End, Bucket) ->
    lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = key(N),
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


%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Errors) ->
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    read_from_cluster(Node, Start, End, Errors, ?TEST_BUCKET, CommonValBin).

read_from_cluster(Node, Start, End, Errors, Bucket, CommonValBin) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = key(N),
            case  riak_client:get(Bucket, Key, C) of
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
                        [length(ErrorsFound)]),
            length(ErrorsFound);
        _ ->
            ?assertEqual(Errors, length(ErrorsFound))
    end.

key(N) ->
    list_to_binary(io_lib:format("~8..0B~n", [N])).