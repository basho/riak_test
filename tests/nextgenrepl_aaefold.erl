%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_aaefold).
-behavior(riak_test).
-export([confirm/0]).
-export([fullsync_check/3]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(C_RING, 16).
-define(A_NVAL, 1).
-define(B_NVAL, 3).
-define(C_NVAL, 2).

-define(SNK_WORKERS, 8).
-define(VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

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

    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {1, ?CONFIG(?A_RING, ?A_NVAL)},
            {3, ?CONFIG(?B_RING, ?B_NVAL)},
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
    setup_replqueues(ClusterA, [cluster_b, cluster_c]),
    setup_replqueues(ClusterB, [cluster_a, cluster_c]),
    setup_replqueues(ClusterC, [cluster_b, cluster_a]),

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

    lager:info("Test empty clusters don't show any differences"),
    [{http, {IPA, PortAH}}, {pb, {IPA, PortAP}}] = rt:connection_info(NodeA),
    [{http, {IPB, PortBH}}, {pb, {IPB, PortBP}}] = rt:connection_info(NodeB),
    [{http, {IPC, PortCH}}, {pb, {IPC, PortCP}}] = rt:connection_info(NodeC),
    
    RefA = {NodeA, IPA, PortAH, ?A_NVAL},
    RefB = {NodeB, IPB, PortBH, ?B_NVAL},
    RefC = {NodeC, IPC, PortCH, ?C_NVAL},

    {root_compare, 0} = fullsync_check(RefA, RefB, no_repair),
    {root_compare, 0} = fullsync_check(RefB, RefC, no_repair),
    {root_compare, 0} = fullsync_check(RefC, RefA, no_repair),

    ok = setup_snkreplworkers(ClusterA, ClusterB, cluster_b),

    lager:info("Test 5000 key difference and resolve"),
    % Write keys to cluster A, verify B and C do not have them.
    FunMod:write_to_cluster(NodeA, 1, 5000, ?TEST_BUCKET, true, ?VAL_INIT),
    FunMod:read_from_cluster(NodeB, 1, 5000, 5000, ?TEST_BUCKET, ?VAL_INIT),
    FunMod:read_from_cluster(NodeC, 1, 5000, 5000, ?TEST_BUCKET, ?VAL_INIT),

    SrcHTTPCA = rhc:create(IPA, PortAH, "riak", []),
    SrcHTTPCB = rhc:create(IPB, PortBH, "riak", []),
    _SrcHTTPCC = rhc:create(IPC, PortCH, "riak", []),
    {ok, SrcPBCA} = riakc_pb_socket:start(IPA, PortAP),
    {ok, SrcPBCB} = riakc_pb_socket:start(IPB, PortBP),
    {ok, _SrcPBCC} = riakc_pb_socket:start(IPC, PortCP),

    {ok, KC1} =
        range_repl_compare(SrcHTTPCA, SrcPBCA,
                            ?TEST_BUCKET, all, all, cluster_b),
    ?assertEqual(5000, KC1),
    {root_compare, 0} =
        wait_for_outcome(?MODULE, fullsync_check, [RefA, RefB, no_repair],
                            {root_compare, 0}, 5),
    FunMod:read_from_cluster(NodeB, 1, 5000, 0, ?TEST_BUCKET, ?VAL_INIT),
    FunMod:read_from_cluster(NodeC, 1, 5000, 5000, ?TEST_BUCKET, ?VAL_INIT),

    lager:info("Replicate a range of 900 keys within the bucket"),
    ok = setup_snkreplworkers(ClusterB, ClusterC, cluster_c),
    StrK = FunMod:key(3001),
    EndK = FunMod:key(3900),
    {ok, KC2} =
        range_repl_compare(SrcHTTPCB, SrcPBCB,
                            ?TEST_BUCKET, {StrK, EndK}, all, cluster_c),
    ?assertEqual(900, KC2),
    0 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeC, 3001, 3900, undefined,
                                ?TEST_BUCKET, ?VAL_INIT],
                            0,
                            5),
    FunMod:read_from_cluster(NodeC, 1, 3000, 3000, ?TEST_BUCKET, ?VAL_INIT),
    FunMod:read_from_cluster(NodeC, 3901, 5000, 1100, ?TEST_BUCKET, ?VAL_INIT),
    
    lager:info("Replicate puts based on modified date range"),
    {MegaB4, SecsB4, _} = os:timestamp(),
    SWbefore = MegaB4 * 1000000 + SecsB4,
    timer:sleep(1000),
    FunMod:write_to_cluster(NodeB, 3901, 4500, ?TEST_BUCKET, false, ?VAL_MOD),
    timer:sleep(1000),
    {MegaAft, SecsAft, _} = os:timestamp(),
    SWafter = MegaAft * 1000000 + SecsAft,
    timer:sleep(1000),
    FunMod:write_to_cluster(NodeB, 4501, 5000, ?TEST_BUCKET, false, ?VAL_MOD),

    {ok, KC3} =
        range_repl_compare(SrcHTTPCB, SrcPBCB,
                            ?TEST_BUCKET,
                            {FunMod:key(3901), FunMod:key(5000)},
                            {SWbefore, SWafter},
                            cluster_c),
    ?assertEqual(600, KC3),
    500 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeC, 3901, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_MOD],
                            500,
                            5),
    {MegaNow, SecsNow, _} = os:timestamp(),
    SWnow = MegaNow * 1000000 + SecsNow,
    {ok, KC4} =
        range_repl_compare(SrcHTTPCB, SrcPBCB,
                            ?TEST_BUCKET,
                            {FunMod:key(3901), FunMod:key(5000)},
                            {SWafter, SWnow},
                            cluster_c),
    ?assertEqual(500, KC4),
    0 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeC, 3901, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_MOD],
                            0,
                            5),
    
    lager:info("Complete all necessary replications using all"),
    
    {ok, KC5} =
        range_repl_compare(SrcHTTPCB, SrcPBCB,
                            ?TEST_BUCKET, all, all, cluster_c),
    ?assertEqual(5000, KC5),
    1100 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeC, 1, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_INIT],
                            1100,
                            5),
    3900 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeC, 1, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_MOD],
                            3900,
                            5),
    lager:info("Replicate back to cluster A changes from B"),
    ok = setup_snkreplworkers(ClusterB, ClusterA, cluster_a),
    {ok, KC6} =
        range_repl_compare(SrcHTTPCB, SrcPBCB,
                            ?TEST_BUCKET, all, all, cluster_a),
    ?assertEqual(5000, KC6),
    1100 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeA, 1, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_INIT],
                            1100,
                            5),
    3900 = 
        wait_for_outcome(FunMod,
                            read_from_cluster,
                            [NodeA, 1, 5000, undefined,
                                ?TEST_BUCKET, ?VAL_MOD],
                            3900,
                            5),

    lager:info("Final validation everything is sync'd"),
    {root_compare, 0} = fullsync_check(RefA, RefB, no_repair),
    {root_compare, 0} = fullsync_check(RefB, RefC, no_repair),
    {root_compare, 0} = fullsync_check(RefC, RefA, no_repair),

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
            {{Acc, 0, IP, Port, http}, Acc + 1}
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

wait_for_outcome(Module, Func, Args, _ExpOutcome, 0) ->
    apply(Module, Func, Args);
wait_for_outcome(Module, Func, Args, ExpOutcome, Loops) ->
    case apply(Module, Func, Args) of
        ExpOutcome ->
            ExpOutcome;
        NotRightYet ->
            lager:info("~w not yet ~w ~w", [Func, ExpOutcome, NotRightYet]),
            timer:sleep(1000),
            wait_for_outcome(Module, Func, Args, ExpOutcome, Loops - 1)
    end.

fullsync_check({SrcNode, _SrcIP, _SrcPort, SrcNVal},
                {_SinkNode, SinkIP, SinkPort, SinkNVal},
                QueueName) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [QueueName]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]).

range_repl_compare(RHC, PBC, B, KR, MR, QN) ->
    RH = rhc:aae_range_replkeys(RHC, B, KR, MR, QN),
    RP = riakc_pb_socket:aae_range_replkeys(PBC, B, KR, MR, QN),
    ?assertEqual(RH, RP),
    lager:info("Range repl answers - HTTP ~p PB ~p", [RH, RP]),
    RH.