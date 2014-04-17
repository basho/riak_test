-module(replication_object_reformat).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"object-reformat">>).
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
             {anti_entropy, {on, []}},
             {anti_entropy_build_limit, {100, 1000}},
             {anti_entropy_concurrency, 100}
            ]
        },
        {riak_repl,
         [
          {fullsync_strategy, aae},
          {fullsync_on_connect, false},
          {fullsync_interval, disabled},
          {max_fssource_retries, Retries}
         ]}
        ]).

confirm() ->
    verify_replication(v0, v1, 1, ?NUM_KEYS),
    verify_replication(v1, v0, 1, ?NUM_KEYS).

verify_replication(AVersion, BVersion, Start, End) ->
    rt:set_advanced_conf(all, ?CONF(infinity)),

    Nodes = [ANodes, BNodes] = rt:build_clusters([3, 3]),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Updating app config to force ~p on source cluster.",
               [AVersion]),
    [rt:update_app_config(N, [{riak_kv,
                               [{object_format, AVersion}]}])
     || N <- ANodes],

    lager:info("Updating app config to force ~p on sink cluster.",
               [BVersion]),
    [rt:update_app_config(N, [{riak_kv,
                               [{object_format, BVersion}]}])
     || N <- BNodes],

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

    lager:info("Enabling fullsync from A to B"),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Wait for capability on source cluster."),
    [rt:wait_until_capability(N, {riak_kv, object_format}, AVersion, v0)
     || N <- ANodes],

    lager:info("Wait for capability on sink cluster."),
    [rt:wait_until_capability(N, {riak_kv, object_format}, BVersion, v0)
     || N <- BNodes],

    lager:info("Ensuring connection from cluster A to B"),
    repl_util:connect_cluster_by_name(LeaderA, BPort, "B"),

    lager:info("Write keys, assert they are not available yet."),
    repl_util:write_to_cluster(AFirst, Start, End, ?TEST_BUCKET),

    lager:info("Verify we can not read the keys on the sink."),
    repl_util:read_from_cluster(BFirst, Start, End, ?TEST_BUCKET, ?NUM_KEYS),

    lager:info("Verify we can read the keys on the source."),
    repl_util:read_from_cluster(AFirst, Start, End, ?TEST_BUCKET, 0),

    lager:info("Performing sacrifice."),
    perform_sacrifice(AFirst, Start),

    repl_util:validate_completed_fullsync(
        LeaderA, BFirst, "B", Start, End, ?TEST_BUCKET),

    rt:clean_cluster(lists:flatten(Nodes)).

%% @doc Required for 1.4+ Riak, write sacrificial keys to force AAE
%%      trees to flush to disk.
perform_sacrifice(Node, Start) ->
    ?assertEqual([], repl_util:do_write(Node, Start, 2000,
                                        <<"sacrificial">>, 1)).
