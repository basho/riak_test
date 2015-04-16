-module(repl_fs_bench).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(DIFF_NUM_KEYS, 10).
-define(FULL_NUM_KEYS, 100).
-define(TEST_BUCKET, <<"repl_bench">>).

-define(HARNESS, (rt_config:get(rt_harness))).

-define(CONF(Strategy), [
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
          {fullsync_strategy, Strategy},
          {fullsync_on_connect, false},
          {fullsync_interval, disabled},
          {max_fssource_retries, infinity},
          {max_fssource_cluster, 1},
          {max_fssource_node, 1},
          {max_fssink_node, 1}
         ]}
        ]).

confirm() ->
    {E1, F1, D1, N1} = fullsync_test(keylist, 0),
    {E2, F2, D2, N2} = fullsync_test(keylist, 10),
    {E3, F3, D3, N3} = fullsync_test(keylist, 100),

    {E4, F4, D4, N4} = fullsync_test(aae, 0),
    {E5, F5, D5, N5} = fullsync_test(aae, 10),
    {E6, F6, D6, N6} = fullsync_test(aae, 100),

    lager:info("Keylist Empty: ~pms ~pms ~pms", [E1 / 1000, E2 / 1000, E3 / 1000]),
    lager:info("Keylist Full:  ~pms ~pms ~pms", [F1 / 1000, F2 / 1000, F3 / 1000]),
    lager:info("Keylist Diff:  ~pms ~pms ~pms", [D1 / 1000, D2 / 1000, D3 / 1000]),
    lager:info("Keylist None:  ~pms ~pms ~pms", [N1 / 1000, N2 / 1000, N3 / 1000]),

    lager:info("AAE Empty: ~pms ~pms ~pms", [E4 / 1000, E5 / 1000, E6 / 1000]),
    lager:info("AAE Full:  ~pms ~pms ~pms", [F4 / 1000, F5 / 1000, F6 / 1000]),
    lager:info("AAE Diff:  ~pms ~pms ~pms", [D4 / 1000, D5 / 1000, D6 / 1000]),
    lager:info("AAE None:  ~pms ~pms ~pms", [N4 / 1000, N5 / 1000, N6 / 1000]),

    pass.

%% @doc Perform a fullsync, with given latency injected via intercept
%%      and return times for each fullsync time.
fullsync_test(Strategy, Latency) ->
    rt:set_advanced_conf(all, ?CONF(Strategy)),

    [ANodes, BNodes] = rt:build_clusters([3, 3]),

    rt:wait_for_cluster_service(ANodes, riak_repl),
    rt:wait_for_cluster_service(BNodes, riak_repl),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    [rt_intercept:load_code(Node) || Node <- ANodes],

    case {Strategy, Latency} of
        {aae, 10} ->
            [rt_intercept:add(Node,
                              {riak_repl_aae_source,
                               [{{get_reply, 1}, delayed_get_reply}]})
             || Node <- ANodes],
            ok;
        {keylist, 10} ->
            [rt_intercept:add(Node,
                              {riak_repl2_fssource,
                               [{{handle_info, 2}, slow_handle_info}]})
             || Node <- ANodes],
            ok;
        {aae, 100} ->
            [rt_intercept:add(Node,
                              {riak_repl_aae_source,
                               [{{get_reply, 1}, really_delayed_get_reply}]})
             || Node <- ANodes],
            ok;
        {keylist, 100} ->
            [rt_intercept:add(Node,
                              {riak_repl2_fssource,
                               [{{handle_info, 2}, really_slow_handle_info}]})
             || Node <- ANodes],
            ok;
        _ ->
            ok
    end,

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

    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {EmptyTime, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    %% Write keys and perform fullsync.
    repl_util:write_to_cluster(AFirst, 0, ?FULL_NUM_KEYS, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    %% Rewrite first 10% keys and perform fullsync.
    repl_util:write_to_cluster(AFirst, 0, ?DIFF_NUM_KEYS, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    %% Write no keys, and perform the fullsync.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {NoneTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    {EmptyTime, FullTime, DiffTime, NoneTime}.
