-module(repl_fs_bench).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DIFF_NUM_KEYS, 1000).
-define(FULL_NUM_KEYS, 10000).
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

-import(rt, [deploy_nodes/2]).

confirm() ->
    {AAEEmpty, AAEFull, AAEDiff, AAENone} = fullsync_test(aae),

    {KeylistEmpty, KeylistFull, KeylistDiff, KeylistNone} = fullsync_test(keylist),

    lager:info("Results for aae:"),
    lager:info("Empty fullsync completed in: ~pms", [AAEEmpty / 1000]),
    lager:info("All fullsync completed in:   ~pms", [AAEFull / 1000]),
    lager:info("Diff fullsync completed in:  ~pms", [AAEDiff / 1000]),
    lager:info("None fullsync completed in:  ~pms", [AAENone / 1000]),

    lager:info("Results for keylist:"),
    lager:info("Empty fullsync completed in: ~pms", [KeylistEmpty / 1000]),
    lager:info("All fullsync completed in:   ~pms", [KeylistFull / 1000]),
    lager:info("Diff fullsync completed in:  ~pms", [KeylistDiff / 1000]),
    lager:info("None fullsync completed in:  ~pms", [KeylistNone / 1000]),

    ?assert(AAEEmpty < KeylistEmpty),
    ?assert(AAEFull < KeylistFull),
    ?assert(AAEDiff < KeylistDiff),
    ?assert(AAENone < KeylistNone),

    pass.

fullsync_test(Strategy) ->
    rt:set_advanced_conf(all, ?CONF(Strategy)),

    {ANodes, BNodes} = case ?HARNESS of
        rtssh ->
            [A, B] = rt:build_clusters([3, 3]),
            {A, B};
        rtdev ->
            Nodes = deploy_nodes(6, ?CONF(Strategy)),

            %% Break up the 6 nodes into three clustes.
            {A, B} = lists:split(3, Nodes),

            lager:info("Building two clusters."),
            [repl_util:make_cluster(N) || N <- [A, B]],
            {A, B}
    end,

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

    lager:info("connect cluster A:~p to B on port ~p",
               [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, IP, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Wait for cluster connection A:~p -> B:~p:~p",
               [LeaderA, BFirst, Port]),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %% Perform fullsync of an empty cluster.
    repl_util:wait_until_aae_trees_built(ANodes ++ BNodes),
    {EmptyTime, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    %% Write keys and perform fullsync.
    write_to_cluster(AFirst, 0, ?FULL_NUM_KEYS),
    repl_util:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    %% Rewrite first 10% keys and perform fullsync.
    write_to_cluster(AFirst, 0, ?DIFF_NUM_KEYS),
    repl_util:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    %% Write no keys, and perform the fullsync.
    repl_util:wait_until_aae_trees_built(ANodes ++ BNodes),
    {NoneTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    {EmptyTime, FullTime, DiffTime, NoneTime}.

write_to_cluster(Node, Start, End) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start, Node]),
    ?assertEqual([],
                 repl_util:do_write(Node, Start, End, ?TEST_BUCKET, 1)).
