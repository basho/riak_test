-module(replication_stats).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(FULL_NUM_KEYS, 5000).
-define(TEST_BUCKET, <<"repl_bench">>).

-define(HARNESS, (rt_config:get(rt_harness))).

-define(CONF, [
        {riak_core,
            [
             {ring_creation_size, 8},
             {default_bucket_props, [{n_val, 1}, {allow_mult, false}]}
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
          {fullsync_strategy, keylist},
          {fullsync_on_connect, false},
          {fullsync_interval, disabled},
          {max_fssource_retries, infinity},
          {max_fssource_cluster, 1},
          {max_fssource_node, 1},
          {max_fssink_node, 1}
         ]}
        ]).

confirm() ->
    fullsync_enabled_and_started().

fullsync_enabled_and_started() ->
    rt:set_advanced_conf(all, ?CONF),

    [ANodes, BNodes] = rt:build_clusters([3, 3]),

    rt:wait_for_cluster_service(ANodes, riak_repl),
    rt:wait_for_cluster_service(BNodes, riak_repl),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

    LeaderA = rpc:call(AFirst,
                       riak_core_cluster_mgr, get_leader, []),

    {ok, {IP, Port}} = rpc:call(BFirst,
                                application, get_env, [riak_core, cluster_mgr]),

    repl_util:connect_cluster(LeaderA, IP, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %% Write keys and perform fullsync.
    repl_util:write_to_cluster(AFirst, 0, ?FULL_NUM_KEYS, ?TEST_BUCKET),

    Me = self(),

    spawn(fun() ->
                {FullTime, _} = timer:tc(repl_util,
                                         start_and_wait_until_fullsync_complete,
                                         [LeaderA, undefined, Me]),
                lager:info("Fullsync completed in ~p", [FullTime]),
                Me ! fullsync_completed
        end),

    StartedResult = receive
        fullsync_started ->
            lager:info("Fullsync started!"),

            case rpc:call(LeaderA, riak_repl_console, fs_remotes_status,
                          []) of
                {badrpc, _} ->
                    badrpc;
                Stats ->
                    ?assertEqual(Stats,
                                 [{fullsync_enabled, "B"},
                                  {fullsync_running, "B"}]),
                    pass
            end
    after 60000 ->
            timeout
    end,

    ?assertEqual(pass, StartedResult),

    _ = receive
        fullsync_completed -> ok
    after 60000 ->
        ?assert(timeout)
    end,

    FullStats = rpc:call(AFirst, riak_repl_console, status, [quiet]),
    CoordinatorStats = proplists:get_value(fullsync_coordinator, FullStats, []),
    BStats = proplists:get_value("B", CoordinatorStats, []),
    LastFullsyncValue = proplists:get_value(last_fullsync_completed, BStats, no_exists),

    ?assertMatch({{_Year, _Month, _Day}, {_Hour, _Minute, _Second}}, LastFullsyncValue),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    StartedResult.
