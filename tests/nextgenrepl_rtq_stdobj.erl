%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_rtq_stdobj).
-behavior(riak_test).
-export([confirm/0]).
-export([fullsync_check/3]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 32).
-define(C_RING, 8).
-define(A_NVAL, 4).
-define(B_NVAL, 2).
-define(C_NVAL, 3).

-define(SNK_WORKERS, 4).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(REPL_SLEEP, 512). 
    % May need to wait for 2 x the 256ms max sleep time of a snk worker
-define(WAIT_LOOPS, 12).

-define(CONFIG(RingSize, NVal, ReplCache), [
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
           {enable_repl_cache, ReplCache}
          ]}
        ]).

confirm() ->
    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, true)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, true)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, false)}]),
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
    test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC).

test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC) ->
    
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
    
    lager:info("Suspend a queue at source and confirm replication stops ..."),
    lager:info("... but continues from unsuspended queues"),
    ok = action_on_srcqueue(ClusterC, cluster_a, suspend_rtq),
    write_to_cluster(NodeC, 1001, 2000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1001, 2000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 1001, 2000, ?COMMMON_VAL_INIT, 1000),
    ok = action_on_srcqueue(ClusterC, cluster_a, resume_rtq),
    lager:info("Resuming the queue changes nothing ..."),
    lager:info("... But new PUTs will now replicate"),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1001, 2000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 1001, 2000, ?COMMMON_VAL_INIT, 1000),
    write_to_cluster(NodeC, 1101, 2000, ?COMMMON_VAL_MOD),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1001, 1100, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeA, 1101, 2000, ?COMMMON_VAL_MOD, 0),
        % errors down to 100
    lager:info("Full sync from another cluster will resolve"),
    {clock_compare, 100} =
        fullsync_check({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL},
                        cluster_a),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1001, 1100, ?COMMMON_VAL_INIT, 0, true),
    read_from_cluster(NodeA, 1101, 2000, ?COMMMON_VAL_MOD, 0),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC}),

    lager:info("Suspend working on a queue from sink and confirm ..."),
    lager:info("... replication stops but continues from unsuspended sinks"),
    ok = action_on_snkqueue(ClusterA, cluster_a, suspend_snkqueue),
    write_to_cluster(NodeC, 2001, 3000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 2001, 3000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 2001, 3000, ?COMMMON_VAL_INIT, 1000),
    ok = action_on_snkqueue(ClusterA, cluster_a, resume_snkqueue),
    lager:info("Resuming the queue prompts recovery ..."),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 2001, 3000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 2001, 3000, ?COMMMON_VAL_INIT, 0),

    lager:info("Stop a node in source - and repl OK"),
    NodeA0 = hd(tl(ClusterA)),
    rt:stop_and_wait(NodeA0),
    lager:info("Node stopped"),
    write_to_cluster(NodeA, 3001, 4000, new_obj),
    read_from_cluster(NodeA, 3001, 4000, ?COMMMON_VAL_INIT, 0),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeB, 3001, 4000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 3001, 4000, ?COMMMON_VAL_INIT, 0),

    lager:info("Node restarting"),
    rt:start_and_wait(NodeA0),
    rt:wait_for_service(NodeA0, riak_kv),

    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair],
                            {root_compare, 0},
                            ?WAIT_LOOPS),
    
    lager:info("Stop a node in sink - and repl OK"),
    rt:stop_and_wait(NodeA0),
    lager:info("Node stopped"),
    write_to_cluster(NodeB, 4001, 5000, new_obj),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeA, 4001, 5000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 4001, 5000, ?COMMMON_VAL_INIT, 0),
    lager:info("Node restarting"),
    rt:start_and_wait(NodeA0),
    rt:wait_for_service(NodeA0, riak_kv),


    lager:info("Kill a node in source - and repl OK"),
    rt:brutal_kill(NodeA0),
    lager:info("Node killed"),
    timer:sleep(1000), % Cluster may settle after kill
    write_to_cluster(NodeA, 5001, 8000, new_obj),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeB, 5001, 8000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 5001, 8000, ?COMMMON_VAL_INIT, 0),

    lager:info("Confirm replication from cache-less cluster ..."),
    lager:info(".. with node still killed in cluster A"),
    write_to_cluster(NodeC, 8001, 10000, new_obj),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL}, 
                                no_repair],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeB, 8001, 10000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 8001, 10000, ?COMMMON_VAL_INIT, 0),

    pass.


action_on_srcqueue([], _SnkClusterName, _Action) ->
    ok;
action_on_srcqueue([SrcNode|Rest], SnkClusterName, Action) ->
    ok = rpc:call(SrcNode, riak_kv_replrtq_src, Action, [SnkClusterName]),
    action_on_srcqueue(Rest, SnkClusterName, Action).

action_on_snkqueue([], _SnkClusterName, _Action) ->
    ok;
action_on_snkqueue([SnkNode|Rest], SnkClusterName, Action) ->
    ok = rpc:call(SnkNode, riak_kv_replrtq_snk, Action, [SnkClusterName]),
    action_on_snkqueue(Rest, SnkClusterName, Action).

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


setup_snkreplworkers(SrcCluster, SnkNodes, SnkName) ->
    PeerMap =
        fun(Node, Acc) ->
            {http, {IP, Port}} =
                lists:keyfind(http, 1, rt:connection_info(Node)),
            {{Acc, 0, IP, Port}, Acc + 1}
        end,
    {PeerList, _} = lists:mapfoldl(PeerMap, 1, SrcCluster),
    SetupSnkFun = 
        fun(Node) ->
            ok = rpc:call(Node,
                            riak_kv_replrtq_snk,
                            add_snkqueue,
                            [SnkName, PeerList, ?SNK_WORKERS])
        end,
    lists:foreach(SetupSnkFun, SnkNodes).


fullsync_check({SrcNode, SrcIP, SrcPort, SrcNVal},
                {_SinkNode, SinkIP, SinkPort, SinkNVal},
                SnkClusterName) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_source, [http, SrcIP, SrcPort]),
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
            case length(ErrorsFound) of
                Errors ->
                    ok;
                _ ->
                    lists:foreach(fun(E) -> lager:warning("Read error ~w", [E]) end, ErrorsFound)
            end,
            ?assertEqual(Errors, length(ErrorsFound))
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