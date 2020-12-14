%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_ttaaefs_rangesync).
-behavior(riak_test).
-export([confirm/0]).
-export([delete_from_cluster/3]).
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
           {ttaaefs_maxresults, 32},
           {ttaaefs_rangeboost, 4}, 
           {delete_mode, keep}
          ]}
        ]).

confirm() ->
    [ClusterA1, ClusterB1, ClusterC1] = setup_clusters(),
    test_repl_between_clusters(ClusterA1, ClusterB1, ClusterC1).

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


test_repl_between_clusters(ClusterA, ClusterB, ClusterC) ->
    
    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

    RangeCheckFun = rangesync_checkfun(),
    
    ok = setup_replqueues(ClusterA ++ ClusterB ++ ClusterC),

    lager:info("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    {http, {IPC, PortC}} = lists:keyfind(http, 1, rt:connection_info(NodeC)),
    lager:info("Cluster A ~s ~w Cluster B ~s ~w Cluster C ~s ~w",
                [IPA, PortA, IPB, PortB, IPC, PortC]),
    
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0}
        = fullsync_check({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {root_compare, 0}
        = fullsync_check({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),
    lager:info("Root compare has not set range check"),
    lists:foreach(fun(N) -> ?assertMatch(none, get_range(N)) end,
                    [NodeA, NodeB, NodeC]),
    
    lager:info("Range check should also root compare"),
    {root_compare, 0}
        = RangeCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0}
        = RangeCheckFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {root_compare, 0}
        = RangeCheckFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),


    lager:info("Test 100 key difference and resolve"),
    % Write keys to cluster A, verify B and C do not have them.
    write_to_cluster(NodeA, 1, 100),
    read_from_cluster(NodeB, 1, 100, 100),
    read_from_cluster(NodeC, 1, 100, 100),
    {clock_compare, 32}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    ?assertMatch(?TEST_BUCKET, element(1, get_range(NodeA))),
    ?assertMatch(all, element(2, get_range(NodeA))),
    ?assertMatch(none, get_range(NodeB)),
    lager:info("Range check now resolves A -> B"),
    {clock_compare, 68}
        = RangeCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    ?assertMatch(?TEST_BUCKET, element(1, get_range(NodeA))),
    ?assertMatch(all, element(2, get_range(NodeA))),
    {root_compare, 0}
        = RangeCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    ?assertMatch(none, get_range(NodeA)),

    lager:info("On startup - range_check should check since startup"),
    lager:info("Everything should be fixed due to range boost"),
    {clock_compare, 100}
        = RangeCheckFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {tree_compare, 0}
        = partialsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeC, IPC, PortC, ?C_NVAL},
                            all_check,
                            os:timestamp()),
    
    lager:info("Cluster B should be N=3 and nodes=2, so can cope with node out of coverage"),
    ok = rpc:call(NodeB, riak_client, remove_node_from_coverage, []),
    timer:sleep(1000),
    {tree_compare, 0}
        = partialsync_check({NodeB, IPB, PortB, ?B_NVAL},
                            {NodeC, IPC, PortC, ?C_NVAL},
                            all_check,
                            os:timestamp()),
    ok = rpc:call(NodeB, riak_client, reset_node_for_coverage, []),
    ok = rpc:call(NodeA, riak_client, remove_node_from_coverage, []),
    timer:sleep(1000),

    lager:info("something bad now happens with NodeA out of coverage"),
    {error, 0}
        = partialsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeC, IPC, PortC, ?C_NVAL},
                            all_check,
                            os:timestamp()),

    pass.


get_range(Node) ->
    rpc:call(Node, riak_kv_ttaaefs_manager, get_range, []).

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
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]),
    SrcHTTPC = rhc:create(SrcIP, SrcPort, "riak", []),
    {ok, SnkC} = riak:client_connect(SinkNode),
    N = drain_queue(SrcHTTPC, SnkC),
    lager:info("Drained queue and pushed ~w objects", [N]),
    AAEResult.

rangesync_checkfun() ->
    fun({SrcNode, SrcIP, SrcPort, SrcNVal},
            {SinkNode, SinkIP, SinkPort, SinkNVal}) ->
        ModRef = riak_kv_ttaaefs_manager,
        _ = rpc:call(SrcNode, ModRef, pause, []),
        ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
        ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
        AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [range_check, 60]),
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