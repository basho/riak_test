%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(repl_ttaaefs_semiauto).
-behavior(riak_test).
-export([confirm/0]).
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
    test_repl_between_clusters(ClusterA, ClusterB, ClusterC,
                                fun fullsync_check/2,
                                fun setup_replqueues/1).


test_repl_between_clusters(ClusterA, ClusterB, ClusterC,
                                FullSyncFun,
                                SetupReplFun) ->
    repl_ttaaefs_manual:test_repl_between_clusters(ClusterA,
                                                    ClusterB,
                                                    ClusterC, 
                                                    FullSyncFun,
                                                    SetupReplFun).

setup_replqueues([]) ->
    ok;
setup_replqueues([HeadNode|Others]) ->
    true = rpc:call(HeadNode, riak_kv_replrtq_src, register_rtq, [q1_ttaaefs, any]),
    setup_replqueues(Others).


fullsync_check({SrcNode, SrcIP, SrcPort, SrcNVal},
                {SinkNode, SinkIP, SinkPort, SinkNVal}) ->
    {http, {IPSrc, PortSrc}} =
        lists:keyfind(http, 1, rt:connection_info(SrcNode)),
    ok = rpc:call(SinkNode, riak_kv_replrtq_snk,
                    add_snkqueue, [q1_ttaaefs, [{1, 0, IPSrc, PortSrc}], 8]),

    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, [ModRef]),
    ok = rpc:call(SrcNode, ModRef, set_source, [ModRef, http, SrcIP, SrcPort]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [ModRef, http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [ModRef, SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]),

    lager:info("Sleeping to await queue drain."),
    timer:sleep(2000),

    ok = rpc:call(SinkNode, riak_kv_replrtq_snk,
                    remove_snkqueue, [q1_ttaaefs]),

    AAEResult.

