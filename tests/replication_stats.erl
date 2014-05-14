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
    Nodes = [ANodes, _BNodes] = configure_and_start_replication(),

    AFirst = hd(ANodes),

    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    %% Check fullsync statistics.
    Result = receive
        fullsync_started ->
            lager:info("Fullsync started!"),

            case rpc:call(LeaderA, riak_repl_console, fs_remotes_status,
                          []) of
                {badrpc, _} ->
                    fail;
                FsStats ->
                    ?assertEqual(FsStats,
                                 [{fullsync_enabled, "B"},
                                  {fullsync_running, "B"}]),
                    pass
            end
    after 60000 ->
            fail
    end,

    ?assertEqual(pass, Result),

    %% Wait for socket to accumulate multiple statistics for histogram.
    timer:sleep(5000),

    %% Check socket statistics.
    StatString = os:cmd(io_lib:format("curl -s -S ~s/riak-repl/stats",
                                      [rt:http_url(LeaderA)])),
    {struct, Stats} = mochijson2:decode(StatString),
    {struct, FsCoordStats} = proplists:get_value(<<"fullsync_coordinator">>, Stats),
    {struct, BStats} = proplists:get_value(<<"B">>, FsCoordStats),
    {struct, BSocketStats} = proplists:get_value(<<"socket">>, BStats),

    [verify_type(Attribute) || Attribute <- BSocketStats],

    reset_clusters(Nodes),

    pass.

verify_type({Attribute, Value}) when is_binary(Attribute) ->
    ListStats = [<<"recv_avg">>,
                 <<"recv_cnt">>,
                 <<"recv_dvi">>,
                 <<"recv_kbps">>,
                 <<"recv_max">>,
                 <<"send_cnt">>,
                 <<"send_kbps">>,
                 <<"send_pend">>],

    lager:info("Verifying ~p is correct type with value ~p.",
               [Attribute, Value]),

    Result = case lists:member(Attribute, ListStats) of
        true ->
            lager:info("Attribute ~p should be a list: ~p",
                       [Attribute, Value]),
            if
                is_list(Value) ->
                    pass;
                true ->
                    fail
            end;
        false ->
            pass
    end,

    ?assertEqual(pass, Result).

reset_clusters(Clusters) ->
    [rt:clean_cluster(Nodes) || Nodes <- Clusters].

configure_and_start_replication() ->
    rt:set_advanced_conf(all, ?CONF),

    [ANodes, BNodes] = rt:build_clusters([3, 3]),

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
                lager:info("Fullsync completed in ~p", [FullTime])
        end),

    [ANodes, BNodes].
