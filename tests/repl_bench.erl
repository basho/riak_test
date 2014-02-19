-module(repl_bench).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl_bench">>).
-define(CONF, [
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
          {max_fssource_retries, infinity}
         ]}
        ]).

confirm() ->
    rt:set_advanced_conf(all, ?CONF),
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
    {EmptyTime, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),

    %% Write 10000 keys and perform fullsync.
    write_to_cluster(AFirst, 0, 10000),
    {AllTime, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),

    %% Rewrite first 1000 keys and perform fullsync.
    write_to_cluster(AFirst, 0, 1000),
    {DiffTime, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),

    lager:info("***********************************************************************"),
    lager:info("Empty fullsync completed in: ~p", [EmptyTime]),
    lager:info("All fullsync completed in:   ~p", [AllTime]),
    lager:info("Diff fullsync completed in:  ~p", [DiffTime]),

    pass.

write_to_cluster(Node, Start, End) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start, Node]),
    ?assertEqual([], repl_util:do_write(Node, Start, End, ?TEST_BUCKET, 1)).
