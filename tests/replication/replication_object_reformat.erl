-module(replication_object_reformat).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"object-reformat">>).
-define(NUM_KEYS,    1000).
-define(N,           3).

-define(CONF(Retries), [
        {riak_core,
            [
             {ring_creation_size, 8},
             {default_bucket_props, [{n_val, ?N}]}
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
    lager:info("Verifying v0 source to v1 sink, realtime enabled."),
    verify_replication(v0, v1, 1, ?NUM_KEYS, true),
    lager:info("Verifying v0 source to v1 sink, realtime disabled."),
    verify_replication(v0, v1, 1, ?NUM_KEYS, false),
    lager:info("Verifying v1 source to v0 sink, realtime enabled."),
    verify_replication(v1, v0, 1, ?NUM_KEYS, true),
    lager:info("Verifying v1 source to v0 sink, realtime disabled."),
    verify_replication(v1, v0, 1, ?NUM_KEYS, false),
    pass.

%% @doc Verify replication works between two different versions of the
%%      Riak object format.
verify_replication(AVersion, BVersion, Start, End, Realtime) ->
    [ANodes, BNodes] = configure_clusters(AVersion, BVersion, Realtime),

    Nodes = [ANodes, BNodes],

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    lager:info("Get leader of A cluster."),
    LeaderA = repl_util:get_leader(AFirst),

    %% Before starting writes, initiate a rolling downgrade.
    Me = self(),

    case Realtime of
        true ->
            spawn(fun() ->
                          lager:info("Running kv_reformat to downgrade to v0 on ~p",
                                     [BFirst]),
                          {_, _, Error1} = rpc:call(BFirst,
                                                    riak_kv_reformat,
                                                    run,
                                                    [v0, [{kill_handoffs, false}]]),
                          ?assertEqual(0, Error1),

                          lager:info("Waiting for all nodes to see the v0 capability."),
                          [rt:wait_until_capability(N, {riak_kv, object_format}, v0, v0)
                           || N <- BNodes],

                          lager:info("Allowing downgrade and writes to occurr concurrently."),
                          Me ! continue,

                          lager:info("Downgrading node ~p to previous.",
                                     [BFirst]),
                          rt:upgrade(BFirst, previous),

                          lager:info("Waiting for riak_kv to start on node ~p.",
                                     [BFirst]),
                          rt:wait_for_service(BFirst, [riak_kv])
                  end),
            ok;
        _ ->
            ok
    end,

    %% Pause and wait for rolling upgrade to begin, if it takes too
    %% long, proceed anyway and the test will fail when it attempts
    %% to read the keys.
    receive
        continue ->
            ok
    after 60000 ->
            ok
    end,

    lager:info("Write keys, assert they are not available yet."),
    repl_util:write_to_cluster(AFirst, Start, End, ?TEST_BUCKET, ?N),

    case Realtime of
        false ->
            lager:info("Verify we can not read the keys on the sink."),
            repl_util:read_from_cluster(
                BFirst, Start, End, ?TEST_BUCKET, ?NUM_KEYS, ?N);
        _ ->
            ok
    end,

    lager:info("Verify we can read the keys on the source."),
    repl_util:read_from_cluster(AFirst, Start, End, ?TEST_BUCKET, 0, ?N),

    %% Wait until the sink cluster is in a steady state before
    %% starting fullsync
    rt:wait_until_nodes_ready(BNodes),
    rt:wait_until_no_pending_changes(BNodes),
    rt:wait_until_registered(BFirst, riak_repl2_fs_node_reserver),

    repl_util:validate_completed_fullsync(
        LeaderA, BFirst, "B", Start, End, ?TEST_BUCKET),

    lager:info("Verify we can read the keys on the sink."),
    repl_util:read_from_cluster(BFirst, Start, End, ?TEST_BUCKET, 0, ?N),

    %% Verify if we downgrade sink, after replication has complete, we
    %% can still read the objects.
    %%
    case {Realtime, BVersion} of
        {false, v1} ->
            lager:info("Running kv_reformat to downgrade to v0 on ~p",
                       [BFirst]),
            {_, _, Error} = rpc:call(BFirst,
                                     riak_kv_reformat,
                                     run,
                                     [v0, [{kill_handoffs, false}]]),
            ?assertEqual(0, Error),

            lager:info("Waiting for all nodes to see the v0 capability."),
            [rt:wait_until_capability(N, {riak_kv, object_format}, v0, v0)
             || N <- BNodes],

            lager:info("Downgrading node ~p to previous.",
                       [BFirst]),
            rt:upgrade(BFirst, previous),

            lager:info("Waiting for riak_kv to start on node ~p.",
                       [BFirst]),
            rt:wait_for_service(BFirst, riak_kv),

            lager:info("Verify we can read from node ~p after downgrade.",
                       [BFirst]),
            repl_util:read_from_cluster(
                BFirst, Start, End, ?TEST_BUCKET, 0, ?N),
            ok;
        _ ->
            ok
    end,

    rt_cluster:clean_cluster(lists:flatten(Nodes)).

%% @doc Configure two clusters and set up replication between them,
%%      return the node list of each cluster.
configure_clusters(AVersion, BVersion, Realtime) ->
    rt_config:set_advanced_conf(all, ?CONF(infinity)),

    Nodes = [ANodes, BNodes] = rt_cluster:build_clusters([3, 3]),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Updating app config to force ~p on source cluster.",
               [AVersion]),
    [rt_config:update_app_config(N, [{riak_kv,
                               [{object_format, AVersion}]}])
     || N <- ANodes],

    lager:info("Updating app config to force ~p on sink cluster.",
               [BVersion]),
    [rt_config:update_app_config(N, [{riak_kv,
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

    lager:info("Connecting cluster A to B"),
    {ok, {BIP, BPort}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),

    repl_util:connect_cluster(LeaderA, BIP, BPort),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    lager:info("Enabling fullsync from A to B"),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    case Realtime of
        true ->
            lager:info("Enabling realtime from A to B"),
            repl_util:enable_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            rt:wait_until_ring_converged(BNodes);
        _ ->
            ok
    end,

    lager:info("Wait for capability on source cluster."),
    [rt:wait_until_capability(N, {riak_kv, object_format}, AVersion, v0)
     || N <- ANodes],

    lager:info("Wait for capability on sink cluster."),
    [rt:wait_until_capability(N, {riak_kv, object_format}, BVersion, v0)
     || N <- BNodes],

    lager:info("Ensuring connection from cluster A to B"),
    repl_util:connect_cluster_by_name(LeaderA, BPort, "B"),

    Nodes.
