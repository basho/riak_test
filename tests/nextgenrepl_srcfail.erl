%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_srcfail).
-behavior(riak_test).
-export([confirm/0]).
-export([fullsync_check/3]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 1).

-define(SNK_WORKERS, 8).
-define(VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(WAIT_LOOPS, 12).

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
    FunMod = nextgenrepl_ttaaefs_manual,

    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {5, ?CONFIG(?A_RING, ?A_NVAL)},
            {1, ?CONFIG(?B_RING, ?B_NVAL)}]),
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),
    
    lager:info("Ready for test."),
    setup_replqueues(ClusterA, [cluster_b]),
    setup_replqueues(ClusterB, [cluster_a]),

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),

    lager:info("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    
    RefA = {NodeA, IPA, PortA, ?A_NVAL},
    RefB = {NodeB, IPB, PortB, ?B_NVAL},

    {root_compare, 0} = fullsync_check(RefA, RefB, no_repair),
    {root_compare, 0} = fullsync_check(RefB, RefA, no_repair),

    ok = setup_snkreplworkers(ClusterA, ClusterB, cluster_b),

    lager:info("Test 5000 key difference and resolve"),
    % Write keys to cluster A, verify B does not have them
    FunMod:write_to_cluster(NodeA, 1, 5000, ?TEST_BUCKET, true, ?VAL_INIT),
    FunMod:read_from_cluster(NodeB, 1, 5000, 5000, ?TEST_BUCKET, ?VAL_INIT),

    SrcHTTPCA = rhc:create(IPA, PortA, "riak", []),
    _SrcHTTPCB = rhc:create(IPB, PortB, "riak", []),

    {ok, KC1} =
        rhc:aae_range_replkeys(SrcHTTPCA, ?TEST_BUCKET, all, all, cluster_b),
    ?assertEqual(5000, KC1),
    {root_compare, 0} =
        wait_for_outcome(?MODULE, fullsync_check, [RefA, RefB, no_repair],
                            {root_compare, 0}, ?WAIT_LOOPS),
    FunMod:read_from_cluster(NodeB, 1, 5000, 0, ?TEST_BUCKET, ?VAL_INIT),

    lager:info("Modify all, then replicate some of the keys"),
    FunMod:write_to_cluster(NodeA, 1, 5000, ?TEST_BUCKET, false, ?VAL_MOD),

    StrK = FunMod:key(3001),
    EndK = FunMod:key(3900),
    {ok, KC2} =
        rhc:aae_range_replkeys(SrcHTTPCA,
                                ?TEST_BUCKET, {StrK, EndK}, all,
                                cluster_b),
    ?assertEqual(900, KC2),
    FunMod:read_from_cluster(NodeB, 1, 3000, 3000, ?TEST_BUCKET, ?VAL_MOD),
    FunMod:read_from_cluster(NodeB, 3901, 5000, 1100, ?TEST_BUCKET, ?VAL_MOD),
    0 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeB, 3001, 3900, undefined,
                                ?TEST_BUCKET, ?VAL_MOD],
                            0,
                            ?WAIT_LOOPS),
    
    lager:info("Fail a source-side node - replicate more keys"),
    FailNode1 = lists:nth(2, ClusterA),
    FailNode2 = lists:nth(3, ClusterA),
    rt:stop_and_wait(FailNode1),
    {ok, KC3} =
        rhc:aae_range_replkeys(SrcHTTPCA,
                                ?TEST_BUCKET,
                                {FunMod:key(3901), FunMod:key(5000)},
                                all,
                                cluster_b),
    ?assertEqual(1100, KC3),
    0 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeB, 3001, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_MOD],
                            0,
                            ?WAIT_LOOPS),
    {ok, KC4} =
        rhc:aae_range_replkeys(SrcHTTPCA,
                                ?TEST_BUCKET,
                                all,
                                all,
                                cluster_b),
    ?assertEqual(5000, KC4),
    0 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeB, 1, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_MOD],
                            0,
                            ?WAIT_LOOPS),

    lager:info("Validate everything is sync'd"),
    {root_compare, 0} = fullsync_check(RefA, RefB, no_repair),
    {root_compare, 0} = fullsync_check(RefB, RefA, no_repair),

    lager:info("Restart and check everything is in sync"),
    rt:start_and_wait(FailNode1),
    rt:wait_for_service(FailNode1, riak_kv),
    {root_compare, 0} = fullsync_check(RefA, RefB, no_repair),
    {root_compare, 0} = fullsync_check(RefB, RefA, no_repair),


    lager:info("Load additional keys - to replicate via AAE after stop"),
    FunMod:write_to_cluster(NodeA, 5001, 6000, ?TEST_BUCKET, true, ?VAL_INIT),
    FunMod:read_from_cluster(NodeB, 5001, 6000, 1000, ?TEST_BUCKET, ?VAL_INIT),
    rt:stop_and_wait(FailNode1),
    {root_compare, 0} =
        wait_for_outcome(?MODULE, fullsync_check, [RefA, RefB, cluster_b],
                            {root_compare, 0}, ?WAIT_LOOPS),
    rt:start_and_wait(FailNode1),
    rt:wait_for_service(FailNode1, riak_kv),

    lager:info("Load additional keys - to replicate via AAE after kill"),
    FunMod:write_to_cluster(NodeA, 6001, 7000, ?TEST_BUCKET, true, ?VAL_INIT),
    FunMod:read_from_cluster(NodeB, 6001, 7000, 1000, ?TEST_BUCKET, ?VAL_INIT),
    rt:brutal_kill(FailNode2),
    {root_compare, 0} =
        wait_for_outcome(?MODULE, fullsync_check, [RefA, RefB, cluster_b],
                            {root_compare, 0}, ?WAIT_LOOPS),

    pass.


setup_replqueues([], _ClusterList) ->
    ok;
setup_replqueues([HeadNode|Others], ClusterList) ->
    SetupQFun = 
        fun(ClusterName) ->
            true = rpc:call(HeadNode,
                            riak_kv_replrtq_src,
                            register_rtq,
                            [ClusterName, block_rtq])
        end,
    lists:foreach(SetupQFun, ClusterList),
    setup_replqueues(Others, ClusterList).

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

fullsync_check({SrcNode, SrcIP, SrcPort, SrcNVal},
                {_SinkNode, SinkIP, SinkPort, SinkNVal},
                QueueName) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [QueueName]),
    ok = rpc:call(SrcNode, ModRef, set_source, [http, SrcIP, SrcPort]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]).

