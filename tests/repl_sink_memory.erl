-module(repl_sink_memory).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(DIFF_NUM_KEYS, 10).
-define(FULL_NUM_KEYS, 100).
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
          {rt_heartbeat_interval, 10},
          {max_fssource_retries, infinity},
          {max_fssource_cluster, 1},
          {max_fssource_node, 1},
          {max_fssink_node, 1}
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
    rt:wait_until_ring_converged(BNodes),

    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    %% Prohibit heartbeat responses on all nodes on the sink.
    [rt_intercept:load_code(Node) || Node <- BNodes],
    [suspend_heartbeat_responses(Node) || Node <- BNodes],

    %% Begin writing a ton of objects in another thread.
    Me = self(),

    spawn(fun() ->
                repl_util:write_to_cluster(AFirst, 0, 100000, ?TEST_BUCKET),
                Me ! completed
        end),

    %% Wait for writes to complete and monitor memory on the sink.
    LeaderB = repl_util:get_leader(BFirst),
    wait_for_writes_to_complete(LeaderB),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    pass.

%% @doc Wait for writes to complete, and print memory every 60 seconds.
wait_for_writes_to_complete(Node) ->
    receive
        completed ->
            ok
    after 60 ->
            Memory = rpc:call(Node, erlang, memory, [binary]),
            lager:warning("Current sink memory on leader: ~p", [Memory]),
            wait_for_writes_to_complete(Node)
    end.

%% @doc Prohibit delivery of the heartbeat response messages.
suspend_heartbeat_responses(Node) ->
    lager:info("Suspending sending of heartbeat responses from node ~p", [Node]),
    rt_intercept:add(Node, {riak_repl2_rtsink_conn,
                            [{{send_heartbeat, 2}, drop_send_heartbeat_resp}]}).
