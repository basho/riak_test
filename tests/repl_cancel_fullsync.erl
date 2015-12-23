-module(repl_cancel_fullsync).
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
             {default_bucket_props,
                 [
                     {n_val, 1},
                     {allow_mult, true},
                     {dvv_enabled, true}
                 ]}
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
          {max_fssource_retries, Retries}
         ]}
        ]).

%% @doc Ensure we can cancel a fullsync and restart it.
confirm() ->
    rt:set_advanced_conf(all, ?CONF(5)),

    Nodes = [ANodes, BNodes] = rt:build_clusters([3, 3]),

    rt:wait_for_cluster_service(ANodes, riak_repl),
    rt:wait_for_cluster_service(BNodes, riak_repl),

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

    lager:info("Test fullsync from cluster A leader ~p to cluster B",
               [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Starting fullsync."),
    rt:log_to_nodes(Nodes, "Starting fullsync."),
    R1 = rt:rpc_call(LeaderA, riak_repl_console, fullsync, [["start"]]),
    ?assertEqual(ok, R1),
    repl_util:wait_until_fullsync_started(LeaderA),
    lager:info("Fullsync running."),

    %% Get all active keylist server pids
    Coordinators = [Pid || {"B", Pid} <-
        riak_repl2_fscoordinator_sup:started(LeaderA)],
    States = [sys:get_state(P) || P <- Coordinators],
    KeylistPids = lists:flatten([element(14, State) || State <- States]),
    KLStates = [sys:get_state(Pid) || {Pid, _} <- KeylistPids],
    [?assertEqual(state, element(1, State)) || State <- KLStates],

    lager:info("Stopping fullsync."),
    rt:log_to_nodes(Nodes, "Stopping fullsync."),
    R2 = rt:rpc_call(LeaderA, riak_repl_console, fullsync, [["stop"]]),
    ?assertEqual(ok, R2),
    repl_util:wait_until_fullsync_stopped(LeaderA),
    lager:info("Fullsync stopped."),

    %% Give keylist pids time to stop
    timer:sleep(500),
    %% Ensure keylist pids are actually gone
    Exits = [catch sys:get_state(Pid) || {Pid, _} <- KeylistPids],
    [?assertMatch({'EXIT', _}, Exit) || Exit <- Exits],

    [{"B", S1}] = rt:rpc_call(LeaderA, riak_repl2_fscoordinator, status, []),
    ?assertEqual(true, lists:member({fullsyncs_completed, 0}, S1)),
    lager:info("Fullsync not completed."),

    [{"B", S2}] = rt:rpc_call(LeaderA, riak_repl2_fscoordinator, status, []),
    ?assertEqual(true, lists:member({in_progress, 0}, S2)),
    lager:info("** ~p", [S2]),

    lager:info("Starting fullsync."),
    rt:log_to_nodes(Nodes, "Starting fullsync."),
    R3 = rt:rpc_call(LeaderA, riak_repl_console, fullsync, [["start"]]),
    ?assertEqual(ok, R3),
    repl_util:wait_until_fullsync_started(LeaderA),
    lager:info("Fullsync running again."),

    Res = rt:wait_until(LeaderA,
        fun(_) ->
                Status = rt:rpc_call(LeaderA,
                                  riak_repl_console,
                                  status,
                                  [quiet]),
                case proplists:get_value(server_fullsyncs, Status) of
                    1 ->
                        true;
                    _ ->
                        false
                end
        end),
    ?assertEqual(ok, Res),
    repl_util:read_from_cluster(BFirst, 1, ?NUM_KEYS, ?TEST_BUCKET, 0),
    [{"B", S3}] = rt:rpc_call(LeaderA, riak_repl2_fscoordinator, status, []),
    ?assertEqual(true, lists:member({fullsyncs_completed, 1}, S3)),
    lager:info("Fullsync Complete"),

    rt:log_to_nodes(Nodes, "Test completed."),
    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes),

    pass.
