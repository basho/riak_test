%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(repl_aae_fullsync).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(NUM_KEYS,    1000).

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
             {sweep_tick, 10000},
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
          {max_fssource_soft_retries, 10},
          {max_fssource_retries, Retries}
         ]}
        ]).

confirm() ->
    difference_test(),
    deadlock_test(),
    simple_test(),
    bidirectional_test(),
    dual_test(),
    pass.

simple_test() ->
    lager:info("Starting simple_test"),

    %% Deploy 6 nodes.
    Nodes = rt:deploy_nodes(6, ?CONF(5), [riak_kv, riak_repl]),

    %% Break up the 6 nodes into three clustes.
    {ANodes, BNodes} = lists:split(3, Nodes),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Building two clusters."),
    [repl_util:make_cluster(N) || N <- [ANodes, BNodes]],

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
    LeaderA = get_leader(AFirst),
    LeaderB = get_leader(BFirst),

    lager:info("Finding connection manager ports."),
    BPort = get_port(LeaderB),

    lager:info("Connecting cluster A to B"),
    connect_cluster(LeaderA, BPort, "B"),

    %% Write keys prior to fullsync.
    write_to_cluster(AFirst, 1, ?NUM_KEYS),

    %% Read keys prior to fullsync.
    read_from_cluster(BFirst, 1, ?NUM_KEYS, ?NUM_KEYS),

    %% Wait for trees to compute.
    rt:wait_until_aae_trees_built(ANodes),
    rt:wait_until_aae_trees_built(BNodes),

    lager:info("Test fullsync from cluster A leader ~p to cluster B",
               [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    TargetA = hd(ANodes -- [LeaderA]),
    TargetB = hd(BNodes),

    %% Flush AAE trees to disk.
    perform_sacrifice(AFirst),

    %% Validate replication from A -> B is fault-tolerant regardless of
    %% errors occurring on the source or destination.
    validate_intercepted_fullsync(TargetA, LeaderA, "B"),
    validate_intercepted_fullsync(TargetB, LeaderA, "B"),

    %% Verify data is replicated from A -> B successfully once the
    %% intercepts are removed.
    validate_completed_fullsync(LeaderA, BFirst, "B", 1, ?NUM_KEYS),

    rt:clean_cluster(Nodes),

    pass.

dual_test() ->
    lager:info("Starting dual_test"),

    %% Deploy 6 nodes.
    Nodes = rt:deploy_nodes(6, ?CONF(infinity), [riak_kv, riak_repl]),

    %% Break up the 6 nodes into three clustes.
    {ANodes, Rest} = lists:split(2, Nodes),
    {BNodes, CNodes} = lists:split(2, Rest),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),
    lager:info("CNodes: ~p", [CNodes]),

    lager:info("Building three clusters."),
    [repl_util:make_cluster(N) || N <- [ANodes, BNodes, CNodes]],

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    CFirst = hd(CNodes),

    lager:info("Naming clusters."),
    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),
    repl_util:name_cluster(CFirst, "C"),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    lager:info("Waiting for transfers to complete."),
    rt:wait_until_transfers_complete(ANodes),
    rt:wait_until_transfers_complete(BNodes),
    rt:wait_until_transfers_complete(CNodes),

    lager:info("Get leaders."),
    LeaderA = get_leader(AFirst),
    LeaderB = get_leader(BFirst),
    LeaderC = get_leader(CFirst),

    lager:info("Finding connection manager ports."),
    APort = get_port(LeaderA),
    BPort = get_port(LeaderB),
    CPort = get_port(LeaderC),

    lager:info("Connecting all clusters into fully connected topology."),
    connect_cluster(LeaderA, BPort, "B"),
    connect_cluster(LeaderA, CPort, "C"),
    connect_cluster(LeaderB, APort, "A"),
    connect_cluster(LeaderB, CPort, "C"),
    connect_cluster(LeaderC, APort, "A"),
    connect_cluster(LeaderC, BPort, "B"),

    %% Write keys to cluster A, verify B and C do not have them.
    write_to_cluster(AFirst, 1, ?NUM_KEYS),
    read_from_cluster(BFirst, 1, ?NUM_KEYS, ?NUM_KEYS),
    read_from_cluster(CFirst, 1, ?NUM_KEYS, ?NUM_KEYS),

    %% Enable fullsync from A to B.
    lager:info("Enabling fullsync from A to B"),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    %% Enable fullsync from A to C.
    lager:info("Enabling fullsync from A to C"),
    repl_util:enable_fullsync(LeaderA, "C"),
    rt:wait_until_ring_converged(ANodes),

    %% Wait for trees to compute.
    rt:wait_until_aae_trees_built(ANodes),
    rt:wait_until_aae_trees_built(BNodes),
    rt:wait_until_aae_trees_built(CNodes),

    %% Flush AAE trees to disk.
    perform_sacrifice(AFirst),

    %% Verify data is replicated from A -> B successfully
    validate_completed_fullsync(LeaderA, BFirst, "B", 1, ?NUM_KEYS),

    %% Verify data is replicated from A -> C successfully
    validate_completed_fullsync(LeaderA, CFirst, "C", 1, ?NUM_KEYS),

    write_to_cluster(AFirst,
                     ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS),
    read_from_cluster(BFirst,
                      ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS, ?NUM_KEYS),
    read_from_cluster(CFirst,
                      ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS, ?NUM_KEYS),

    %% Verify that duelling fullsyncs eventually complete
    {Time, _} = timer:tc(repl_util,
                         start_and_wait_until_fullsync_complete,
                         [LeaderA]),

    read_from_cluster(BFirst, ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS, 0),
    read_from_cluster(CFirst, ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS, 0),
    lager:info("Fullsync A->B and A->C completed in ~p seconds",
               [Time/1000/1000]),

    pass.

bidirectional_test() ->
    lager:info("Starting bidirectional_test"),

    %% Deploy 6 nodes.
    Nodes = rt:deploy_nodes(6, ?CONF(5), [riak_kv, riak_repl]),

    %% Break up the 6 nodes into three clustes.
    {ANodes, BNodes} = lists:split(3, Nodes),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Building two clusters."),
    [repl_util:make_cluster(N) || N <- [ANodes, BNodes]],

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
    LeaderA = get_leader(AFirst),
    LeaderB = get_leader(BFirst),

    lager:info("Finding connection manager ports."),
    APort = get_port(LeaderA),
    BPort = get_port(LeaderB),

    lager:info("Connecting cluster A to B"),
    connect_cluster(LeaderA, BPort, "B"),

    lager:info("Connecting cluster B to A"),
    connect_cluster(LeaderB, APort, "A"),

    %% Write keys to cluster A, verify B does not have them.
    write_to_cluster(AFirst, 1, ?NUM_KEYS),
    read_from_cluster(BFirst, 1, ?NUM_KEYS, ?NUM_KEYS),

    %% Enable fullsync from A to B.
    lager:info("Enabling fullsync from A to B"),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    %% Enable fullsync from B to A.
    lager:info("Enabling fullsync from B to A"),
    repl_util:enable_fullsync(LeaderB, "A"),
    rt:wait_until_ring_converged(BNodes),

    %% Flush AAE trees to disk.
    perform_sacrifice(AFirst),

    %% Wait for trees to compute.
    rt:wait_until_aae_trees_built(ANodes),

    %% Verify A replicated to B.
    validate_completed_fullsync(LeaderA, BFirst, "B", 1, ?NUM_KEYS),

    %% Write keys to cluster B, verify A does not have them.
    write_to_cluster(AFirst, ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS),
    read_from_cluster(BFirst, ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS, ?NUM_KEYS),

    %% Flush AAE trees to disk.
    perform_sacrifice(BFirst),

    %% Wait for trees to compute.
    rt:wait_until_aae_trees_built(BNodes),

    %% Verify B replicated to A.
    validate_completed_fullsync(LeaderB, AFirst, "A", ?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS),

    %% Clean.
    rt:clean_cluster(Nodes),

    pass.

difference_test() ->
    lager:info("Starting difference_test"),

    %% Deploy 6 nodes.
    Nodes = rt:deploy_nodes(6, ?CONF(5), [riak_kv, riak_repl]),

    %% Break up the 6 nodes into three clustes.
    {ANodes, BNodes} = lists:split(3, Nodes),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Building two clusters."),
    [repl_util:make_cluster(N) || N <- [ANodes, BNodes]],

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
    LeaderA = get_leader(AFirst),
    LeaderB = get_leader(BFirst),

    lager:info("Finding connection manager ports."),
    BPort = get_port(LeaderB),

    lager:info("Connecting cluster A to B"),
    connect_cluster(LeaderA, BPort, "B"),

    %% Get PBC connections.
    APBC = rt:pbc(LeaderA),
    BPBC = rt:pbc(LeaderB),

    %% Write key.
    ok = riakc_pb_socket:put(APBC,
                             riakc_obj:new(<<"foo">>, <<"bar">>,
                                           <<"baz">>),
                             [{timeout, 4000}]),

    %% Wait for trees to compute.
    rt:wait_until_aae_trees_built(ANodes),
    rt:wait_until_aae_trees_built(BNodes),

    lager:info("Test fullsync from cluster A leader ~p to cluster B",
               [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    %% Flush AAE trees to disk.
    perform_sacrifice(AFirst),

    %% Wait for fullsync.
    {Time1, _} = timer:tc(repl_util,
                         start_and_wait_until_fullsync_complete,
                         [LeaderA, "B"]),
    lager:info("Fullsync completed in ~p seconds", [Time1/1000/1000]),

    %% Read key from after fullsync.
    {ok, O1} = riakc_pb_socket:get(BPBC, <<"foo">>, <<"bar">>,
                                  [{timeout, 4000}]),
    ?assertEqual(<<"baz">>, riakc_obj:get_value(O1)),

    %% Put, generate sibling.
    ok = riakc_pb_socket:put(APBC,
                             riakc_obj:new(<<"foo">>, <<"bar">>,
                                           <<"baz2">>),
                             [{timeout, 4000}]),

    %% Wait for fullsync.
    {Time2, _} = timer:tc(repl_util,
                         start_and_wait_until_fullsync_complete,
                         [LeaderA, "B"]),
    lager:info("Fullsync completed in ~p seconds", [Time2/1000/1000]),

    %% Read key from after fullsync.
    {ok, O2} = riakc_pb_socket:get(BPBC, <<"foo">>, <<"bar">>,
                                  [{timeout, 4000}]),
    ?assertEqual([<<"baz">>, <<"baz2">>], lists:sort(riakc_obj:get_values(O2))),

    rt:clean_cluster(Nodes),

    pass.

deadlock_test() ->
    lager:info("Starting deadlock_test"),

    %% Deploy 6 nodes.
    Nodes = rt:deploy_nodes(6, ?CONF(5), [riak_kv, riak_repl]),

    %% Break up the 6 nodes into three clustes.
    {ANodes, BNodes} = lists:split(3, Nodes),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Building two clusters."),
    [repl_util:make_cluster(N) || N <- [ANodes, BNodes]],

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
    LeaderA = get_leader(AFirst),
    LeaderB = get_leader(BFirst),

    lager:info("Finding connection manager ports."),
    BPort = get_port(LeaderB),

    lager:info("Connecting cluster A to B"),
    connect_cluster(LeaderA, BPort, "B"),

    %% Add intercept for delayed comparison of hashtrees.
    Intercept = {riak_kv_index_hashtree, [{{compare, 4}, delayed_compare}]},
    [ok = rt_intercept:add(Target, Intercept) || Target <- ANodes],

    %% Wait for trees to compute.
    rt:wait_until_aae_trees_built(ANodes),
    rt:wait_until_aae_trees_built(BNodes),

    lager:info("Test fullsync from cluster A leader ~p to cluster B",
               [LeaderA]),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    %% Start fullsync.
    lager:info("Starting fullsync to cluster B."),
    rpc:call(LeaderA, riak_repl_console, fullsync, [["start", "B"]]),

    %% Wait for fullsync to initialize and the AAE repl processes to
    %% stall from the suspended intercepts.
    %% TODO: What can be done better here?
    timer:sleep(25000),

    %% Attempt to get status from fscoordintor.
    Result = rpc:call(LeaderA, riak_repl2_fscoordinator, status, [], 500),
    lager:info("Status result: ~p", [Result]),
    ?assertNotEqual({badrpc, timeout}, Result),

    rt:clean_cluster(Nodes),

    pass.

%% @doc Required for 1.4+ Riak, write sacrificial keys to force AAE
%%      trees to flush to disk.
perform_sacrifice(Node) ->
    ?assertEqual([], repl_util:do_write(Node, 1, 2000,
                                        <<"sacrificial">>, 1)).

%% @doc Validate fullsync completed and all keys are available.
validate_completed_fullsync(ReplicationLeader,
                            DestinationNode,
                            DestinationCluster,
                            Start,
                            End) ->
    ok = check_fullsync(ReplicationLeader, DestinationCluster, 0),
    lager:info("Verify: Reading ~p keys repl'd from A(~p) to ~p(~p)",
               [?NUM_KEYS, ReplicationLeader,
                DestinationCluster, DestinationNode]),
    ?assertEqual(0,
                 repl_util:wait_for_reads(DestinationNode,
                                          Start,
                                          End,
                                          ?TEST_BUCKET,
                                          1)).

%% @doc Assert we can perform one fullsync cycle, and that the number of
%%      expected failures is correct.
check_fullsync(Node, Cluster, ExpectedFailures) ->
    {Time, _} = timer:tc(repl_util,
                         start_and_wait_until_fullsync_complete,
                         [Node, Cluster]),
    lager:info("Fullsync completed in ~p seconds", [Time/1000/1000]),

    Status = rpc:call(Node, riak_repl_console, status, [quiet]),

    Props = case proplists:get_value(fullsync_coordinator, Status) of
        [{_Name, Props0}] ->
            Props0;
        Multiple ->
            {_Name, Props0} = lists:keyfind(Cluster, 1, Multiple),
            Props0
    end,

    %% check that the expected number of partitions failed to sync
    ?assertEqual(ExpectedFailures,
                 proplists:get_value(error_exits, Props)),

    ok.

%% @doc Validate fullsync handles errors for all possible intercept
%%      combinations.
validate_intercepted_fullsync(InterceptTarget,
                              ReplicationLeader,
                              ReplicationCluster) ->
    NumIndicies = length(rpc:call(InterceptTarget,
                                  riak_core_ring,
                                  my_indices,
                                  [rt:get_ring(InterceptTarget)])),
    lager:info("~p owns ~p indices",
               [InterceptTarget, NumIndicies]),

    %% Before enabling fullsync, ensure trees on one source node return
    %% not_built to defer fullsync process.
    validate_intercepted_fullsync(InterceptTarget,
                                  {riak_kv_index_hashtree,
                                   [{{get_lock, 2}, not_built}]},
                                  ReplicationLeader,
                                  ReplicationCluster,
                                  NumIndicies),

    %% Before enabling fullsync, ensure trees on one source node return
    %% already_locked to defer fullsync process.
    validate_intercepted_fullsync(InterceptTarget,
                                  {riak_kv_index_hashtree,
                                   [{{get_lock, 2}, already_locked}]},
                                  ReplicationLeader,
                                  ReplicationCluster,
                                  NumIndicies),

    %% Emulate in progress ownership transfers.
    validate_intercepted_fullsync(InterceptTarget,
                                  {riak_kv_vnode,
                                   [{{hashtree_pid, 1}, wrong_node}]},
                                  ReplicationLeader,
                                  ReplicationCluster,
                                  NumIndicies).

%% @doc Add an intercept on a target node to simulate a given failure
%%      mode, and then enable fullsync replication and verify completes
%%      a full cycle.  Subsequently reboot the node.
validate_intercepted_fullsync(InterceptTarget,
                              Intercept,
                              ReplicationLeader,
                              ReplicationCluster,
                              NumIndicies) ->
    lager:info("Validating intercept ~p on ~p.",
               [Intercept, InterceptTarget]),

    %% Add intercept.
    ok = rt_intercept:add(InterceptTarget, Intercept),

    %% Verify fullsync.
    ok = check_fullsync(ReplicationLeader,
                        ReplicationCluster,
                        NumIndicies),

    %% Reboot node.
    rt:stop_and_wait(InterceptTarget),
    rt:start_and_wait(InterceptTarget),

    %% Wait for riak_kv and riak_repl to initialize.
    rt:wait_for_service(InterceptTarget, riak_kv),
    rt:wait_for_service(InterceptTarget, riak_repl),

    %% Wait until AAE trees are compueted on the rebooted node.
    rt:wait_until_aae_trees_built([InterceptTarget]).

%% @doc Given a node, find the port that the cluster manager is
%%      listening on.
get_port(Node) ->
    {ok, {_IP, Port}} = rpc:call(Node,
                                 application,
                                 get_env,
                                 [riak_core, cluster_mgr]),
    Port.

%% @doc Given a node, find out who the current replication leader in its
%%      cluster is.
get_leader(Node) ->
    rpc:call(Node, riak_core_cluster_mgr, get_leader, []).

%% @doc Connect two clusters using a given name.
connect_cluster(Source, Port, Name) ->
    lager:info("Connecting ~p to ~p for cluster ~p.",
               [Source, Port, Name]),
    repl_util:connect_cluster(Source, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Source, Name)).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start, Node]),
    ?assertEqual([],
                 repl_util:do_write(Node, Start, End, ?TEST_BUCKET, 1)).

%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Errors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start, Node]),
    Res2 = rt:systest_read(Node, Start, End, ?TEST_BUCKET, 1),
    ?assertEqual(Errors, length(Res2)).
