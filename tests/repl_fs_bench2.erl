-module(repl_fs_bench2).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).

%% -define(NUM_KEYS, 100000).
-define(NUM_KEYS, 10000).
-define(TEST_BUCKET, <<"repl_bench">>).
-define(N_VALUE, 3).
-define(Q_VALUE, 8).

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
    Modes = [{aae, true, buffered, 1000, 5},
             {aae, true, inline,   1000, 5}
             ],
%%    Delays = [0, 10, 50, 100, 200],
    Delays = [0],
    Results = [{Strategy, Delay, bench(Strategy, Delay)} || Strategy <- Modes,
                                                            Delay <- Delays],
    io:format("==================================================~n"
              "~p~n"
              "==================================================~n", [Results]),
    pass.

bench({Strategy, Pipeline, DirectMode, DirectLimit, DiffPercent}, Delay) ->
    Config = [{riak_core, [{ring_creation_size, ?Q_VALUE},
                           {default_bucket_props, [{n_val, ?N_VALUE},
                                                   {allow_mult, false}]}]},
              {riak_kv, [{anti_entropy, {on, []}},
                         {anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100}]},
              {riak_repl, [{fullsync_strategy, Strategy},
                           {fullsync_pipeline, Pipeline},
                           {fullsync_direct_limit, DirectLimit},
                           {fullsync_direct_mode, DirectMode},
                           {fullsync_direct_percentage_limit, DiffPercent},
                           {fullsync_on_connect, false},
                           {max_fssource_retries, infinity},
                           {max_fssource_cluster, 1},
                           {max_fssource_node, 1},
                           {max_fssink_node, 1}]}],

    [ANodes, BNodes] = rt:build_clusters([{1, Config},
                                          {1, Config}]),

    repl_util:activate_debug_for_validate_aae_fullsync(ANodes ++ BNodes),

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
    
    LeaderA = repl_aae_fullsync_util:prepare_cluster(ANodes, BNodes),

    %% Perform fullsync of an empty cluster.
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {EmptyTime, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    %% Write keys and perform fullsync.
    Start100 = erlang:now(),
    repl_util:write_to_cluster(AFirst, 1, ?NUM_KEYS, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {FullTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),
    End100 = erlang:now(),
    repl_util:validate_aae_fullsync(Start100, End100, ?N_VALUE, ?Q_VALUE, ?NUM_KEYS, ?NUM_KEYS div 1),

    %% Rewrite first 10% keys and perform fullsync.
    Start10 = erlang:now(),
    repl_util:write_to_cluster(AFirst, 1, ?NUM_KEYS div 10, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),
    End10 = erlang:now(),
    repl_util:validate_aae_fullsync(Start10, End10, ?N_VALUE, ?Q_VALUE, ?NUM_KEYS, ?NUM_KEYS div 10),

    %% Rewrite first 1% keys and perform fullsync.
    Start1 = erlang:now(),
    repl_util:write_to_cluster(AFirst, 1, ?NUM_KEYS div 100, ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime2, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),
    End1 = erlang:now(),
    repl_util:validate_aae_fullsync(Start1, End1, ?N_VALUE, ?Q_VALUE, ?NUM_KEYS, ?NUM_KEYS div 100),

    %% Rewrite first 0.1% keys and perform fullsync.
    Start01 = erlang:now(),
    repl_util:write_to_cluster(AFirst, 1, max(1,?NUM_KEYS div 1000), ?TEST_BUCKET),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {DiffTime3, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),
    End01 = erlang:now(),
    repl_util:validate_aae_fullsync(Start01, End01, ?N_VALUE, ?Q_VALUE, ?NUM_KEYS, ?NUM_KEYS div 1000),


    %% Write no keys, and perform the fullsync.
    Start0 = erlang:now(),
    rt:wait_until_aae_trees_built(ANodes ++ BNodes),
    {NoneTime, _} = timer:tc(repl_util,
                             start_and_wait_until_fullsync_complete,
                             [LeaderA]),
    End0 = erlang:now(),
    repl_util:validate_aae_fullsync(Start0, End0, ?N_VALUE, ?Q_VALUE, ?NUM_KEYS, 0),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    {EmptyTime, FullTime, DiffTime1, DiffTime2, DiffTime3, NoneTime}.
