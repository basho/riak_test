%% @doc Verify that location_down messages during replication occur
%%      and are handled correctly.

-module(repl_location_failures).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-location-failures-systest_a">>).
-define(NUM_KEYS,    1000).

-define(CONF(Retries), [
        {riak_core,
            [
             {ring_creation_size, 8},
             {default_bucket_props, [{n_val, 1}]}
            ]
        },
        {riak_kv,
            [
             %% Specify fast building of AAE trees
             {anti_entropy, {on, []}},
             {anti_entropy_build_limit, {100, 1000}},
             {anti_entropy_concurrency, 100}
            ]
        },
        {riak_repl,
         [
          {fullsync_strategy, keylist},
          {fullsync_on_connect, false},
          {fullsync_interval, disabled},
          {max_reserve_retries, Retries}
         ]}
        ]).

confirm() ->
    rt_config:set_advanced_conf(all, ?CONF(5)),

    [ANodes, BNodes] = rt:build_clusters([3, 3]),

    rt:wait_for_cluster_service(ANodes, riak_repl),
    rt:wait_for_cluster_service(BNodes, riak_repl),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    lager:info("Naming clusters."),
    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Waiting for transfers to complete."),
    rt:wait_until_transfers_complete(ANodes),
    rt:wait_until_transfers_complete(BNodes),

    lager:info("Get leaders."),
    LeaderA = repl_util:get_leader(AFirst),
    LeaderB = repl_util:get_leader(BFirst),

    lager:info("Finding connection manager ports."),
    BPort = repl_util:get_port(LeaderB),

    lager:info("Connecting cluster A to B"),
    repl_util:connect_cluster_by_name(LeaderA, BPort, "B"),

    %% Write keys prior to fullsync.
    repl_util:write_to_cluster(AFirst, 1, ?NUM_KEYS, ?TEST_BUCKET),

    %% Read keys prior to fullsync.
    repl_util:read_from_cluster(BFirst, 1, ?NUM_KEYS, ?TEST_BUCKET,
                                ?NUM_KEYS),

    lager:info("Test fullsync from cluster A leader ~p to cluster B",
               [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    BIndicies = length(rpc:call(LeaderB,
                                riak_core_ring,
                                my_indices,
                                [rt:get_ring(LeaderB)])),

    lager:warning("BIndicies: ~p", [BIndicies]),

    repl_util:validate_intercepted_fullsync(LeaderB,
                                            {riak_repl2_fs_node_reserver,
                                             [{{handle_call, 3},
                                                down_reserve}]},
                                            LeaderA,
                                            "B",
                                            BIndicies),


    %% Verify data is replicated from A -> B successfully once the
    %% intercepts are removed.
    repl_util:validate_completed_fullsync(LeaderA, BFirst, "B", 1,
                                          ?NUM_KEYS, ?TEST_BUCKET),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    pass.
