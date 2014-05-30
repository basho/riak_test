-module(repl_rt_heartbeat_interval).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET,
        <<"repl-cancel-fullsync-failures-systest_a">>).
-define(NUM_KEYS, 1000).

-define(CONF(Retries), [
        {riak_core,
            [
             {ring_creation_size, 8},
             {default_bucket_props, [{n_val, 1}]}
            ]
        },
        {riak_kv,
            [
             %% Specify fast building of AAE trees
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
          {realtime_cascades, never},
          {max_fssource_retries, Retries},
          {rt_heartbeat_interval, disabled}
         ]}
        ]).

%% @doc Ensure we can cancel a fullsync and restart it.
confirm() ->
    rt:set_advanced_conf(all, ?CONF(5)),

    Nodes = [ANodes, BNodes] = rt:build_clusters([3, 3]),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

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
    LeaderB = repl_util:get_leader(BFirst),

    lager:info("Finding connection manager ports."),
    BPort = repl_util:get_port(LeaderB),

    lager:info("Connecting cluster A to B"),
    repl_util:connect_cluster_by_name(LeaderA, BPort, "B"),

    repl_util:write_to_cluster(AFirst, 1, ?NUM_KEYS, ?TEST_BUCKET),

    repl_util:read_from_cluster(BFirst, 1, ?NUM_KEYS, ?TEST_BUCKET,
                                ?NUM_KEYS),

    lager:info("Enabling realtime replication."),
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Starting realtime replication."),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    Me = self(),

    spawn(fun() ->
                repl_util:write_to_cluster(AFirst,
                                           100000, 200000, ?TEST_BUCKET),
                Me ! complete
        end),

    wait_for_keys(BFirst),

    rt:log_to_nodes(Nodes, "Test completed."),

    WaitTime = 120000,
    lager:info("Sleeping for ~p seconds", [WaitTime/1000]),
    timer:sleep(WaitTime),
    Result = rpc:call(BFirst, erlang, memory, [binary]),
    lager:info("Result: ~p", [Result]),
    Tabs = rpc:call(BFirst, ets, all, []),
    EtsState = [rpc:call(BFirst, ets, info, [Tab, memory]) || Tab <- Tabs],
    lager:info("ETS State is ~p", [EtsState]),
    lager:info("Number of processes = ~p", [length(rpc:call(BFirst, erlang,
                    processes, []))]),

    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    pass.

wait_for_keys(Node) ->
    receive
        complete ->
            lager:info("Done!")
    after 500 ->
            Result = rpc:call(Node, erlang, memory, [binary]),
            lager:info("Result: ~p", [Result]),
            Tabs = rpc:call(Node, ets, all, []),
            EtsState = [rpc:call(Node, ets, info, [Tab, memory]) || Tab <- Tabs],
            lager:info("ETS State is ~p", [EtsState]),
            lager:info("Number of processes = ~p",
                [length(rpc:call(Node, erlang, processes, []))]),
            wait_for_keys(Node)
    end.
