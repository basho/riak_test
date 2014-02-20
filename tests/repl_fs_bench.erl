-module(repl_fs_bench).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DIFF_NUM_KEYS, 1000).
-define(FULL_NUM_KEYS, 10000).
-define(TEST_BUCKET, <<"repl_bench">>).

-define(CONF(Strategy), [
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
          {fullsync_strategy, Strategy},
          {fullsync_on_connect, false},
          {fullsync_interval, disabled},
          {max_fssource_retries, infinity}
         ]}
        ]).

confirm() ->
    {None, Full, Diff} = fullsync_test(aae),

    lager:info("Results:"),
    lager:info("Empty fullsync completed in: ~pms", [None / 1000]),
    lager:info("All fullsync completed in:   ~pms", [Full / 1000]),
    lager:info("Diff fullsync completed in:  ~pms", [Diff / 1000]),

    pass.

fullsync_test(Strategy) ->
    rt:set_advanced_conf(all, ?CONF(Strategy)),
    [ANodes, BNodes] = rt:build_clusters([3, 3]),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    lager:info("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),

    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    {ok, {IP, Port}} = rpc:call(BFirst, application, get_env,
                                [riak_core, cluster_mgr]),

    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, IP, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Wait for cluster connection A:~p -> B:~p:~p", [LeaderA, BFirst, Port]),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %% Perform fullsync of an empty cluster.
    repl_util:wait_until_aae_trees_built(ANodes ++ BNodes),
    {NoneTime, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),

    %% Write keys and perform fullsync.
    write_to_cluster(AFirst, 0, ?FULL_NUM_KEYS),
    repl_util:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullTime, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),

    %% Rewrite first 10% keys and perform fullsync.
    write_to_cluster(AFirst, 0, ?DIFF_NUM_KEYS),
    repl_util:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),

    {NoneTime, FullTime, DiffTime}.

write_to_cluster(Node, Start, End) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start, Node]),
    ?assertEqual([], repl_util:do_write(Node, Start, End, ?TEST_BUCKET, 1)).
