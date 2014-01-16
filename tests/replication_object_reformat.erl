-module(replication_object_reformat).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2]).

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
    verify_replication(v0, v1, 1, ?NUM_KEYS).

verify_replication(AVersion, BVersion, Start, End) ->
    Nodes = deploy_nodes(6, ?CONF(infinity)),

    {ANodes, BNodes} = lists:split(3, Nodes),

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

    lager:info("Building two clusters."),
    [repl_util:make_cluster(N) || N <- [ANodes, BNodes]],

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
    LeaderA = rt:repl_get_leader(AFirst),
    LeaderB = rt:repl_get_leader(BFirst),

    lager:info("Finding connection manager ports."),
    BPort = rt:repl_get_port(LeaderB),

    lager:info("Connecting cluster A to B"),
    rt:repl_connect_cluster(LeaderA, BPort, "B"),

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
    rt:repl_connect_cluster(LeaderA, BPort, "B"),

    lager:info("Write keys, assert they are not available yet."),
    rt:write_to_cluster(AFirst, Start, End, ?TEST_BUCKET),

    lager:info("Verify we can not read the keys on the sink."),
    rt:read_from_cluster(BFirst, Start, End, ?TEST_BUCKET, ?NUM_KEYS),

    lager:info("Verify we can read the keys on the source."),
    rt:read_from_cluster(AFirst, Start, End, ?TEST_BUCKET, 0),

    lager:info("Performing sacrifice."),
    perform_sacrifice(AFirst, Start),

    rt:validate_completed_fullsync(LeaderA, BFirst, "B",
                                   Start, End, ?TEST_BUCKET),

    rt:clean_cluster(Nodes).

%% @doc Required for 1.4+ Riak, write sacrificial keys to force AAE
%%      trees to flush to disk.
perform_sacrifice(Node, Start) ->
    ?assertEqual([], repl_util:do_write(Node, Start, 2000,
                                        <<"sacrificial">>, 1)).
