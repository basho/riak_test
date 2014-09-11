-module(repl_fs_bench2).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).

%% -define(NUM_KEYS, 100000).
-define(NUM_KEYS, 1000).
-define(TEST_BUCKET, <<"repl_bench">>).

-define(INT(Nodes, Mod, Intercepts),
        [rt_intercept:add(Node, {Mod, Intercepts}) || Node <- Nodes]).

-define(I(Fun, Args, Intercept),
        {Fun, {Args, Intercept}}).

confirm() ->
    %% Modes = [{keylist, false, 0},
    %%          {aae, false, 0},
    %%          {aae, true, 0},
    %%          {aae, true, 100},
    %%          {aae, true, 1000}],
    %% Modes = [{aae, true, 0}],
    Modes = [{aae, true, 0},
             {aae, true, 100},
             {aae, true, 1000}],
    Delays = [0, 10, 50, 100, 200],
    %% Delays = [0],
    Results = [{Strategy, Delay, bench(Strategy, Delay)} || Strategy <- Modes,
                                                            Delay <- Delays],
    io:format("==================================================~n"
              "~p~n"
              "==================================================~n", [Results]),
    pass.

bench({Strategy, Pipeline, Direct}, Delay) ->
    Config = [{riak_core, [{ring_creation_size, 8},
                           {default_bucket_props, [{n_val, 1},
                                                   {allow_mult, false}]}]},
              {riak_kv, [{anti_entropy, {on, []}},
                         {anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100}]},
              {riak_repl, [{fullsync_strategy, Strategy},
                           {fullsync_pipeline, Pipeline},
                           {fullsync_direct, Direct},
                           {fullsync_on_connect, false},
                           {fullsync_interval, disabled},
                           {max_fssource_retries, infinity},
                           {max_fssource_cluster, 1},
                           {max_fssource_node, 1},
                           {max_fssink_node, 1}]}],

    [ANodes, BNodes] = rt:build_clusters([{1, Config},
                                          {1, Config}]),

    [rt_intercept:load_code(Node) || Node <- ANodes],

    %% Install intercepts to simulate network latency
    ?INT(ANodes,
         riak_repl_aae_source,
         [?I({async_get_bucket, 4}, [],
             fun(Level, Bucket, IndexN, State) ->
                     put({sent, Level, Bucket}, os:timestamp()),
                     riak_repl_aae_source_orig:async_get_bucket_orig(Level, Bucket, IndexN, State)
             end),

          ?I({wait_get_bucket, 4}, [Delay],
             fun(Level, Bucket, IndexN, State) ->
                     Reply = riak_repl_aae_source_orig:wait_get_bucket_orig(Level, Bucket, IndexN, State),
                     T0 = get({sent, Level, Bucket}),
                     Diff = timer:now_diff(os:timestamp(), T0) div 1000,
                     if Diff >= Delay ->
                             ok;
                        true ->
                             timer:sleep(Delay - Diff)
                     end,
                     Reply
             end),

          ?I({async_get_segment, 3}, [],
             fun(Segment, IndexN, State) ->
                     put({sent, Segment}, os:timestamp()),
                     riak_repl_aae_source_orig:async_get_segment_orig(Segment, IndexN, State)
             end),

          ?I({wait_get_segment, 3}, [Delay],
             fun(Segment, IndexN, State) ->
                     Reply = riak_repl_aae_source_orig:wait_get_segment_orig(Segment, IndexN, State),
                     T0 = get({sent, Segment}),
                     Diff = timer:now_diff(os:timestamp(), T0) div 1000,
                     if Diff >= Delay ->
                             ok;
                        true ->
                             timer:sleep(Delay - Diff)
                     end,
                     Reply
             end)
          ]),

    io:format("~p~n", [{ANodes, BNodes}]),

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

    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {EmptyTime, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    %% Write keys and perform fullsync.
    repl_util:write_to_cluster(AFirst, 0, ?NUM_KEYS, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    %% Rewrite first 10% keys and perform fullsync.
    repl_util:write_to_cluster(AFirst, 0, ?NUM_KEYS div 10, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    %% Rewrite first 1% keys and perform fullsync.
    repl_util:write_to_cluster(AFirst, 0, ?NUM_KEYS div 100, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime2, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    %% Write no keys, and perform the fullsync.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {NoneTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    {EmptyTime, FullTime, DiffTime1, DiffTime2, NoneTime}.
