%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_external_reconcile).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(A_NVAL, 1).
-define(B_NVAL, 3).

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
    [ClusterA1, ClusterB1] = setup_clusters(),
    SLP = test_reconcile_between_clusters(ClusterA1, ClusterB1, pb_seg),
    
    PBids = lists:map(fun({SI, _SH}) -> SI end, SLP),

    rt:clean_cluster(ClusterA1),
    rt:clean_cluster(ClusterB1),

    [ClusterA2, ClusterB2] = setup_clusters(),
    [] = test_reconcile_between_clusters(ClusterA2, ClusterB2, http),

    rt:clean_cluster(ClusterA2),
    rt:clean_cluster(ClusterB2),

    [ClusterA3, ClusterB3] = setup_clusters(),
    [] = test_reconcile_between_clusters(ClusterA3, ClusterB3, pb),
    
    rt:clean_cluster(ClusterA3),
    rt:clean_cluster(ClusterB3),

    [ClusterA4, ClusterB4] = setup_clusters(),
    SLH = test_reconcile_between_clusters(ClusterA4, ClusterB4, http_seg),

    HTids = lists:map(fun({SI, _SH}) -> SI end, SLH),

    ?assertEqual(PBids, HTids),
    
    pass.

setup_clusters() ->
    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL)},
            {4, ?CONFIG(?B_RING, ?B_NVAL)}]),
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),
    
    lager:info("Ready for test."),
    [ClusterA, ClusterB].

test_reconcile_between_clusters(ClusterA, ClusterB, QueueFun) ->
    lager:info("Spoof reconciliation with an external cluster QueueFun=~w",
                [QueueFun]),
    {DrainQueueFun, Protocol} =
        case QueueFun of
            pb ->
                {fun drain_queue_pb/2, pb};
            http ->
                {fun drain_queue_http/2, http};
            pb_seg ->
                {fun drain_queue_seg_pb/2, pb};
            http_seg ->
                {fun drain_queue_seg_http/2, http}
            end,

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),

    lager:info("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    lager:info("Cluster A ~s ~w Cluster B ~s ~w", [IPA, PortA, IPB, PortB]),
    
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    
    nextgenrepl_ttaaefs_manual:write_to_cluster(NodeA, 1, 100),

    lager:info("Discover deltas, and request qeueuing"),

    SegList = 
        fullsync_push({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL},
                        ?TEST_BUCKET,
                        DrainQueueFun,
                        Protocol),
    
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    
    lager:info("Discover deletes, and request qeueuing"),

    nextgenrepl_ttaaefs_manual:delete_from_cluster(NodeA, 1, 50, ?TEST_BUCKET),

    _SegListD = 
        fullsync_push({NodeA, IPA, PortA, ?A_NVAL},
                        {NodeB, IPB, PortB, ?B_NVAL},
                        ?TEST_BUCKET,
                        DrainQueueFun,
                        Protocol),
    
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),


    rt:create_and_activate_bucket_type(NodeA,
                                       <<"nval4">>,
                                       [{n_val, 4},
                                            {allow_mult, false}]),
    rt:create_and_activate_bucket_type(NodeB,
                                       <<"nval4">>,
                                       [{n_val, 4},
                                            {allow_mult, false}]),

    Nv4B = {<<"nval4">>, <<"test_typed_buckets">>},
    CommonValBin = <<"CommonValueToWriteForNV4Objects">>,
    nextgenrepl_ttaaefs_manual:write_to_cluster(NodeA, 1, 100, Nv4B, true, CommonValBin),

    lager:info("Discover deltas, and request qeueuing - typed bucket"),

    _TypeSegList = 
        fullsync_push({NodeA, IPA, PortA, 4},
                        {NodeB, IPB, PortB, 4},
                        Nv4B,
                        DrainQueueFun, 
                        Protocol),
    
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, 4},
                            {NodeB, IPB, PortB, 4}),
    {root_compare, 0}
        = fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
       
    SegList.



fullsync_check({SrcNode, _SrcIP, _SrcPort, SrcNVal},
                {SinkNode, SinkIP, SinkPort, SinkNVal}) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]),
    {ok, SnkC} = riak:client_connect(SinkNode),
    {N, []} = drain_queue_http(SrcNode, SnkC),
    lager:info("Drained queue and pushed ~w objects (check)", [N]),
    AAEResult.


fullsync_push({SrcNode, _SrcIP, _SrcPort, SrcNVal},
                {SnkNode, SinkIP, SinkPort, SinkNVal},
                Bucket,
                DrainQueueFun,
                Protocol) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),

    Body = actual_push(SrcNode, SnkNode, Bucket, Protocol),

    {ok, SnkC} = riak:client_connect(SnkNode),
    {N, SegList} = DrainQueueFun(SrcNode, SnkC),
    lager:info("Drained queue and pushed ~w objects (push)", [N]),
    ExpectedBody = lists:flatten(io_lib:format("Queue q1_ttaaefs: 0 ~w 0", [N])),
    ?assertEqual(ExpectedBody, binary_to_list(Body)),
    SegList.


actual_push(SrcNode, SnkNode, Bucket, http) ->
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(SrcNode)),
    SrcHTTPC = rhc:create(IPA, PortA, "riak", []),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(SnkNode)),
    SnkHTTPC = rhc:create(IPB, PortB, "riak", []),

    {ok, {keysclocks, KCLSrc}} = rhc:aae_range_clocks(SrcHTTPC, Bucket, all, all, all),
    {ok, {keysclocks, KCLSnk}} = rhc:aae_range_clocks(SnkHTTPC, Bucket, all, all, all),
    KCLPsh =
        lists:map(fun({{B, K}, C}) -> {B, K, base64:encode_to_string(C)} end,
            lists:subtract(KCLSrc, KCLSnk)),

    {ok, Body} = rhc:push(SrcHTTPC, <<"q1_ttaaefs">>, KCLPsh),
    Body;
actual_push(SrcNode, SnkNode, Bucket, pb) ->
    {pb, {IPA, PortA}} = lists:keyfind(pb, 1, rt:connection_info(SrcNode)),
    {ok, SrcPBC} = riakc_pb_socket:start(IPA, PortA),
    {pb, {IPB, PortB}} = lists:keyfind(pb, 1, rt:connection_info(SnkNode)),
    {ok, SnkPBC} = riakc_pb_socket:start(IPB, PortB),

    {ok, {keysclocks, KCLSrc}} =
        riakc_pb_socket:aae_range_clocks(SrcPBC, Bucket, all, all, all),
    {ok, {keysclocks, KCLSnk}} =
        riakc_pb_socket:aae_range_clocks(SnkPBC, Bucket, all, all, all),
    KCLPsh =
        lists:map(fun({{B, K}, C}) -> {B, K, C} end,
            lists:subtract(KCLSrc, KCLSnk)),

    {ok, Body} = riakc_pb_socket:push(SrcPBC, <<"q1_ttaaefs">>, KCLPsh),
    riakc_pb_socket:stop(SrcPBC),
    riakc_pb_socket:stop(SnkPBC),
    Body.


drain_queue_pb(SrcNode, SnkClient) ->
    {pb, {IPA, PortA}} = lists:keyfind(pb, 1, rt:connection_info(SrcNode)),
    {ok, SrcPBC} = riakc_pb_socket:start(IPA, PortA),
    R = drain_queue(SrcPBC, SnkClient, riakc_pb_socket, 0),
    riakc_pb_socket:stop(SrcPBC),
    R.

drain_queue_http(SrcNode, SnkClient) ->
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(SrcNode)),
    SrcHTTPC = rhc:create(IPA, PortA, "riak", []),
    drain_queue(SrcHTTPC, SnkClient, rhc, 0).

drain_queue(SrcClient, SnkClient, Mod, N) ->
    case Mod:fetch(SrcClient, <<"q1_ttaaefs">>) of
        {ok, queue_empty} ->
            {N, []};
        {ok, {deleted, _TombClock, RObj}} ->
            {ok, _LMD} = riak_client:push(RObj, true, [], SnkClient),
            drain_queue(SrcClient, SnkClient, Mod, N + 1);
        {ok, RObj} ->
            {ok, _LMD} = riak_client:push(RObj, false, [], SnkClient),
            drain_queue(SrcClient, SnkClient, Mod, N + 1)
    end.

drain_queue_seg_pb(SrcNode, SnkClient) ->
    {pb, {IPA, PortA}} = lists:keyfind(pb, 1, rt:connection_info(SrcNode)),
    {ok, SrcPBC} = riakc_pb_socket:start(IPA, PortA),
    R = drain_queue_seg(SrcPBC, SnkClient, riakc_pb_socket, []),
    riakc_pb_socket:stop(SrcPBC),
    R.

drain_queue_seg_http(SrcNode, SnkClient) ->
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(SrcNode)),
    SrcHTTPC = rhc:create(IPA, PortA, "riak", []),
    drain_queue_seg(SrcHTTPC, SnkClient, rhc, []).

drain_queue_seg(SrcClient, SnkClient, Mod, Acc) ->
    case Mod:fetch(SrcClient, <<"q1_ttaaefs">>, internal_aaehash) of
        {ok, queue_empty} ->
            {length(Acc), Acc};
        {ok, {deleted, _TombClock, RObj, SegID, SegHash}} ->
            {ok, _LMD} = riak_client:push(RObj, true, [], SnkClient),
            drain_queue_seg(SrcClient, SnkClient, Mod, [{SegID, SegHash}|Acc]);
        {ok, {RObj, SegID, SegHash}} ->
            {ok, _LMD} = riak_client:push(RObj, false, [], SnkClient),
            drain_queue_seg(SrcClient, SnkClient, Mod, [{SegID, SegHash}|Acc])
    end.
