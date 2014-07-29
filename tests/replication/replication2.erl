-module(replication2).
-behavior(riak_test).
-export([confirm/0, replication/3]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [join/2,
             log_to_nodes/2,
             log_to_nodes/3,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

confirm() ->

    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 3),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
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
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {diff_batch_size, 10}
             ]}
    ],

    lager:info("Building Clusters A and B"),
    [ANodes, BNodes] = rt:build_clusters([{ClusterASize, Conf}, {NumNodes - ClusterASize, Conf}]),

    %lager:info("Skipping all tests"),
    replication(ANodes, BNodes, false),
    pass.


replication(ANodes, BNodes, Connected) ->

    log_to_nodes(ANodes ++ BNodes, "Starting replication2 test"),
    lager:info("Connection Status: ~p", [Connected]),

    lager:info("Real Time Full Sync Replication test"),
    real_time_replication_test(ANodes, BNodes, Connected),
 
    lager:info("Disconnected cluster Full Sync test"),
    disconnected_cluster_fsync_test(ANodes, BNodes),

    lager:info("Failover tests"),
    master_failover_test(ANodes, BNodes),

    lager:info("Network Partition test"),
    network_partition_test(ANodes, BNodes),

    lager:info("Bucket Sync tests"),
    bucket_sync_test(ANodes, BNodes), 

    lager:info("Offline queueing tests"),
    offline_queueing_tests(ANodes, BNodes),

    lager:info("Protocol Buffer writes during shutdown test"),
    pb_write_during_shutdown(ANodes, BNodes),

    lager:info("HTTP writes during shutdown test"),
    http_write_during_shutdown(ANodes, BNodes),

    lager:info("Tests passed"),

    fin.


%% @doc Real time replication test
%%      Test Cycle:
%%        Write some keys with full sync disabled.
%%        Write some keys with full sync enabled.
%%        Check for keys written prior to full sync.
%%        Check all keys.
real_time_replication_test([AFirst|_] = ANodes, [BFirst|_] = BNodes, Connected) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-real_time_replication_test">>,

    case Connected of
        false ->
            %% Before connecting clusters, write some initial data to Cluster A
            lager:info("Writing 100 keys to ~p", [AFirst]),
            ?assertEqual([], repl_util:do_write(AFirst, 1, 100, TestBucket, 2)),

            repl_util:name_cluster(AFirst, "A"),
            repl_util:name_cluster(BFirst, "B"),

            %% Wait for Cluster naming to converge.
            rt:wait_until_ring_converged(ANodes),
            rt:wait_until_ring_converged(BNodes),

            lager:info("Waiting for leader to converge on cluster A"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
            lager:info("Waiting for leader to converge on cluster B"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

            %% Get the leader for the first cluster.
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []), %% Ask Cluster "A" Node 1 who the leader is.

            {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),

            lager:info("connect cluster A:~p to B on port ~p", [LeaderA, BFirstPort]),
            repl_util:connect_cluster(LeaderA, "127.0.0.1", BFirstPort),
            ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

            repl_util:enable_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            repl_util:start_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            repl_util:enable_fullsync(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes);
    
            %% Cluster "A" and Cluster "B" are now connected with real time and full sync enabled.
        _ ->
            lager:info("Clusters should already be connected"),
            lager:info("Waiting for leader to converge on cluster A"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
            lager:info("Waiting for leader to converge on cluster B"),
            
            ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
            
            lager:info("Leader on cluster A is ~p", [LeaderA]),
            lager:info("BFirst on cluster B is ~p", [BFirst]),
            {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),
            lager:info("B is ~p with port ~p", [BFirst, BFirstPort]) 
    end,

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify replication to Cluster B via realtime"),
    lager:info("Writing 100 keys to Cluster A-LeaderNode: ~p", [LeaderA]), % This export from Case isn't my favorite.
    ?assertEqual([], repl_util:do_write(LeaderA, 101, 200, TestBucket, 2)),

    lager:info("Reading 100 keys written to Cluster A-LeaderNode: ~p from Cluster B-Node: ~p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 200, TestBucket, 2)),

    case Connected of
        false ->
            %% Check that the keys we wrote initially aren't replicated yet as
            %% fullsync_on_connect is disabled.
            lager:info("Check keys written before repl was connected are not present"),
            Res2 = rt:systest_read(BFirst, 1, 100, TestBucket, 2),
            ?assertEqual(100, length(Res2)),

            log_to_nodes(ANodes++BNodes, "Test fullsync with leader ~p", [LeaderA]),
            repl_util:start_and_wait_until_fullsync_complete(LeaderA),

            case rpc:call(LeaderA, init, script_id, []) of
                {"riak", Vsn} when Vsn > "1.4" ->
                    %% check that the number of successful FS source exists matches the number of partitions
                    NumExits = repl_util:get_fs_coord_status_item(LeaderA, "B", successful_exits),
                    NumPartitions = repl_util:num_partitions(LeaderA),
                    ?assertEqual(NumPartitions, NumExits);
                _ ->
                    ok
            end,

            lager:info("Check keys written before repl was connected are present"),
            ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, 200, TestBucket, 2));
        _ ->
            ok
    end.


%% @doc Disconnected Clusters Full Sync Test
%%      Test Cycle:
%%        Disconnect Clusters "A" and "B".
%%        Write 2000 keys to Cluster "A".
%%        Reconnect Clusters "A" and "B" and enable real time and full sync.
%%        Read 2000 keys from Cluster "B".
disconnected_cluster_fsync_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
  
    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-discon_fsync_replication_tests">>,
    {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env,[riak_core, cluster_mgr]),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes, "Starting disconnected full sync test"),

    %% Disconnect Cluster B to disablereal time sync
    lager:info("Disconnect the 2 clusters"),
    repl_util:disable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:disconnect_cluster(LeaderA, "B"),
    repl_util:wait_until_no_connection(LeaderA),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Write 2000 keys"),
    ?assertEqual([], repl_util:do_write(LeaderA, 50000, 52000, TestBucket, 2)),

    lager:info("Reconnect the 2 clusters"),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", BFirstPort),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA)),

    repl_util:start_and_wait_until_fullsync_complete(LeaderA),

    lager:info("Read 2000 keys"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 50000, 52000, TestBucket, 2)).


%% @doc Master Failover Test
%%      Test Cycle:
%%        Stop Cluster "A" leader.
%%        Get new Cluster "A" leader.
%%        Write 100 keys to new Cluster "A" leader.
%%        Verify 100 keys are replicated to Cluster "B".
%%        Get Cluster "B" leader and shut it down.
%%        Get new Cluster "B" leader.
%%        Write 100 keys to Cluster "A".
%%        Verify 100 keys are replicated to Cluster "B".
master_failover_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
  
    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-master_failover_test">>,
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes , "Failover tests"),
    log_to_nodes(ANodes ++ BNodes, "Testing master failover: stopping ~p", [LeaderA]),
    
    lager:info("Testing master failover: stopping ~p", [LeaderA]),
    rt:stop(LeaderA),
    rt:wait_until_unpingable(LeaderA),
    ASecond = hd(ANodes -- [LeaderA]),
    repl_util:wait_until_leader(ASecond),

    LeaderA2 = rpc:call(ASecond, riak_core_cluster_mgr, get_leader, []),

    lager:info("New leader is ~p", [LeaderA2]),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA2)),

    %% ASecond is all nodes w/o LeaderA
    lager:info("Writing 100 more keys to ~p now that the old leader is down", [ASecond]),
    ?assertEqual([], repl_util:do_write(ASecond, 201, 300, TestBucket, 2)),

    %% Verify data is replicated to Cluster "B"
    lager:info("Reading 100 keys written to ~p from ~p", [ASecond, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 201, 300, TestBucket, 2)),

    %% Get the leader for Cluster "B"
    LeaderB = rpc:call(BFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes, "Testing client failover: stopping ~p", [LeaderB]),

    lager:info("Testing client failover: stopping ~p", [LeaderB]),
    rt:stop(LeaderB),
    rt:wait_until_unpingable(LeaderB),
    BSecond = hd(BNodes -- [LeaderB]),
    repl_util:wait_until_leader(BSecond),

    LeaderB2 = rpc:call(BSecond, riak_core_cluster_mgr, get_leader, []),

    lager:info("New leader is ~p", [LeaderB2]),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA2)),

    lager:info("Writing 100 more keys to ~p now that the old leader is down", [ASecond]),
    ?assertEqual([], repl_util:do_write(ASecond, 301, 400, TestBucket, 2)),

    %% Verify data is replicated to Cluster B
    lager:info("Reading 101 keys written to ~p from ~p", [ASecond, BSecond]),
    ?assertEqual(0, repl_util:wait_for_reads(BSecond, 301, 400, TestBucket, 2)),

    log_to_nodes(ANodes ++ BNodes, "Test fullsync with ~p and ~p down", [LeaderA, LeaderB]),
    lager:info("Re-running fullsync with ~p and ~p down", [LeaderA, LeaderB]),

    repl_util:start_and_wait_until_fullsync_complete(LeaderA2),

    %% This says test full sync, but there's never a verification step.
    log_to_nodes(ANodes ++ BNodes, "Test fullsync after restarting ~p", [LeaderA]),

    %% Put everything back to 'normal'.
    lager:info("Nodes restarted"),
    lager:info("Restarting down nodes ~p, ~p", [LeaderA, LeaderB]),
    rt:start(LeaderA),
    rt:start(LeaderB),
    rt:wait_until_pingable(LeaderA),
    rt:wait_until_pingable(LeaderB),
    rt:wait_for_service(LeaderA, [riak_kv, riak_repl]),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA2).


%% @doc Network Partition Test
%%      Test Cycle:
%%        Connect the Cluster "A" leader and swap its cookie for a new cookie.
%%        Disconnect node with new cookie from the rest of the nodes.
%%        Nodes will elect a new leader.
%%        Reset cookie on disconnected node and reconnct.
%%        Write 2 keys to node that was reconnected.
%%        Verify replication of keys to Cluster "B".
network_partition_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-network_partition_test">>,
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    log_to_nodes(ANodes ++ BNodes, "Starting network partition test"),

    %% @todo add stuff
    %% At this point, realtime sync should still work, but, it doesn't because of a bug in 1.2.1
    %% Check that repl leader is LeaderA
    %% Check that LeaderA2 has ceeded socket back to LeaderA

    %%lager:info("Leader: ~p", [rpc:call(ASecond, riak_core_cluster_mgr, get_leader, [])]),
    %%lager:info("LeaderA: ~p", [LeaderA]),
    %%lager:info("LeaderA2: ~p", [LeaderA2]),

    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA)),

    %% Swap cookie on LeaderA to simulate a network partition.
    OldCookie = rpc:call(LeaderA, erlang, get_cookie, []),
    NewCookie = list_to_atom(lists:reverse(atom_to_list(OldCookie))),
    rpc:call(LeaderA, erlang, set_cookie, [LeaderA, NewCookie]),

    [rpc:call(LeaderA, erlang, disconnect_node, [Node]) || Node <- ANodes -- [LeaderA]],
    [rpc:call(Node, erlang, disconnect_node, [LeaderA]) || Node <- ANodes -- [LeaderA]],

    repl_util:wait_until_new_leader(hd(ANodes -- [LeaderA]), LeaderA),
    InterimLeader = rpc:call(LeaderA, riak_core_cluster_mgr, get_leader, []),
    lager:info("Interim leader: ~p", [InterimLeader]),

    rpc:call(LeaderA, erlang, set_cookie, [LeaderA, OldCookie]),

    [rpc:call(LeaderA, net_adm, ping, [Node]) || Node <- ANodes -- [LeaderA]],
    [rpc:call(Node, net_adm, ping, [LeaderA]) || Node <- ANodes -- [LeaderA]],

    %% There's no point in writing anything until the leaders converge, as we
    %% can drop writes in the middle of an election
    repl_util:wait_until_leader_converge(ANodes),
    
    ASecond = hd(ANodes -- [LeaderA]),

    lager:info("Leader: ~p", [rpc:call(ASecond, riak_core_cluster_mgr, get_leader, [])]),
    lager:info("Writing 2 more keys to ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 1301, 1302, TestBucket, 2)),

    %% Verify data is replicated to Cluster "B"
    lager:info("Reading 2 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1301, 1302, TestBucket, 2)),

    log_to_nodes(ANodes ++ BNodes, "Completed network partition test").


%% @doc Bucket Sync Test
%%      Test Cycle:
%%        Make 3 buckets on Cluster "A":
%%          -No Replication
%%          -Real Time only
%%          -Full Sync only
%%        Disable real time replication and disconnect Cluster "A" and Cluster "B"
%%        Write 100 keys to real time only bucket.
%%        Reconnect and reenable real time and full sync between Cluster "A" and Cluster "B"
%%        Write 100 keys to the No Replication bucket
%%        Write 100 keys to the Full Sync only bucket
%%        Verify that Full sync didn't replicate
%%        Verify that real time bucket written to offline didn't replicate
%%        Verify that the No Replication bucket didn't replicate
%%        Restart full sync
%%        Verify full sync bucket replicated
%%        Verify that the Real time keys replicated
%%        Verify that the original real time keys did not replicate
%%        Verify that the No replication bucket didn't replicate.
bucket_sync_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
    
    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    FullsyncOnly = <<TestHash/binary, "-fullsync_only">>,
    RealtimeOnly = <<TestHash/binary, "-realtime_only">>,
    NoRepl = <<TestHash/binary, "-no_replication">>,
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
    {ok, {_IP, BFirstPort}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),

    log_to_nodes(ANodes ++ BNodes, "Starting bucket sync test"),

    replication:make_bucket(ANodes, NoRepl, [{repl, false}]),
    replication:make_bucket(ANodes, RealtimeOnly, [{repl, realtime}]),
    replication:make_bucket(ANodes, FullsyncOnly, [{repl, fullsync}]),

    %% Disable real time and disconnect Cluster "B" to prevent real time replication.
    lager:info("Disconnect the 2 clusters"),
    repl_util:disable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:disconnect_cluster(LeaderA, "B"),
    repl_util:wait_until_no_connection(LeaderA),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Write 100 keys to a real time only bucket"),
    ?assertEqual([], repl_util:do_write(AFirst, 1, 100, RealtimeOnly, 2)),

    lager:info("Reconnect Clusters A and B"),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", BFirstPort),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    ?assertEqual(ok, repl_util:wait_until_connection(LeaderA)),

    log_to_nodes(ANodes ++ BNodes, "Test fullsync and realtime independence"),

    lager:info("write 100 keys to a {repl, false} bucket"),
    ?assertEqual([], repl_util:do_write(AFirst, 1, 100, NoRepl, 2)),

    lager:info("write 100 keys to a fullsync only bucket"),
    ?assertEqual([], repl_util:do_write(AFirst, 1, 100, FullsyncOnly, 2)),

    lager:info("Check the fullsync only bucket didn't replicate the writes"),
    Res6 = rt:systest_read(BFirst, 1, 100, FullsyncOnly, 2),
    ?assertEqual(100, length(Res6)),

    lager:info("Check the realtime only bucket that was written to offline isn't replicated"),
    Res7 = rt:systest_read(BFirst, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res7)),

    lager:info("Check the {repl, false} bucket didn't replicate"),
    Res8 = rt:systest_read(BFirst, 1, 100, NoRepl, 2),
    ?assertEqual(100, length(Res8)),

    %% Do a fullsync, make sure that fullsync_only is replicated, but
    %% realtime_only and no_repl aren't
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),

    lager:info("Check fullsync only bucket is now replicated"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, 100, FullsyncOnly, 2)),

    lager:info("Check realtime only bucket didn't replicate"),
    Res10 = rt:systest_read(BFirst, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res10)),

    lager:info("Write 100 more keys into realtime only bucket on ~p", [AFirst]),
    ?assertEqual([], repl_util:do_write(AFirst, 101, 200, RealtimeOnly, 2)),

    lager:info("Check the realtime keys replicated"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 200, RealtimeOnly, 2)),

    lager:info("Check the older keys in the realtime bucket did not replicate"),
    Res12 = rt:systest_read(BFirst, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res12)),

    lager:info("Check {repl, false} bucket didn't replicate"),
    Res13 = rt:systest_read(BFirst, 1, 100, NoRepl, 2),
    ?assertEqual(100, length(Res13)).


%% @doc Offline Queuing Test
%%      Test Cycle:
%%        Stop real time on Cluster "A"
%%        Write 100 keys to leader on Cluster "A"
%%        Restart real time on Cluster "A"
%%        Verify Keys replicated to Cluster "B" after real time was restarted
%%        Stop real time on Cluster "A"
%%        Verify that 100 keys are NOT on Cluster "A"
%%        Write 100 keys to Cluster "A"
%%        Verify that 100 keys were written to Cluster "A"
%%        Verify that 100 keys are NOT on Cluster "B"
%%        Re-enable real time on Cluster "A"
%%        Verify that 100 keys are available on Cluster "B"
offline_queueing_tests([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-offline_queueing_test">>,

    log_to_nodes(ANodes ++ BNodes, "Testing offline realtime queueing"),
    lager:info("Testing offline realtime queueing"),

    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    lager:info("Stopping realtime, queue will build"),
    repl_util:stop_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Writing 100 keys"),
    ?assertEqual([], repl_util:do_write(LeaderA, 800, 900, TestBucket, 2)),

    lager:info("Starting realtime"),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    lager:info("Reading keys written while repl was stopped"),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 800, 900, TestBucket, 2)),

    log_to_nodes(ANodes ++ BNodes, "Testing realtime migration on node shutdown"),
    lager:info("Testing realtime migration on node shutdown"),
    Target = hd(ANodes -- [LeaderA]),

    lager:info("Stopping realtime, queue will build"),
    repl_util:stop_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Verifying 100 keys are missing from ~p", [Target]),
    repl_util:read_from_cluster(Target, 901, 1000, TestBucket, 100),

    lager:info("Writing 100 keys to ~p", [Target]),
    ?assertEqual([], repl_util:do_write(Target, 901, 1000, TestBucket, 2)),

    lager:info("Verifying 100 keys are read from ~p", [Target]),
    repl_util:read_from_cluster(Target, 901, 1000, TestBucket, 0),

    lager:info("Verifying 100 keys are missing from ~p", [BFirst]),
    repl_util:read_from_cluster(BFirst, 901, 1000, TestBucket, 100),

    io:format("queue status: ~p", [rpc:call(Target, riak_repl2_rtq, status, [])]),

    lager:info("Stopping node ~p", [Target]),

    rt:stop(Target),
    rt:wait_until_unpingable(Target),

    lager:info("Starting realtime"),
    repl_util:start_realtime(LeaderA, "B"),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    lager:info("Verifying 100 keys are now available on ~p", [BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 901, 1000, TestBucket, 2)),
    
    rt:start(Target),
    rt:wait_until_pingable(Target),
    rt:wait_for_service(Target, riak_repl).


%% @doc Protocol Buffer Write During Shutdown
%%      Test Cycle:
%%        Connect to Cluster "A" via PB
%%        Spawn background process to stop Cluster "A" nodes
%%        Write 10,000 keys to Cluster "A"
%%        Verify that there are and equal number of write failures on Cluster "A" and read failures on Cluster "B"
pb_write_during_shutdown([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
    
    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-pb_write_shutdown_test">>,

    log_to_nodes(ANodes ++ BNodes, "Testing protocol buffer writes during shutdown"),

    LeaderA = rpc:call( AFirst, riak_core_cluster_mgr, get_leader, []),
    Target = hd( ANodes -- [LeaderA]),

    ConnInfo = proplists:get_value(Target, rt:connection_info([Target])),
    {IP, Port} = proplists:get_value(pb, ConnInfo),
    lager:info("Connecting to pb socket ~p:~p on ~p", [IP, Port, Target]),
    PBSock = rt:pbc(Target),

    %% do the stop in the background while we're writing keys
    spawn(fun() ->
              timer:sleep(500),
              lager:info("Stopping node ~p again", [Target]),
              rt:stop(Target),
              lager:info("Node stopped")
          end),

    lager:info("Writing 10,000 keys"),
    WriteErrors =
        try
            pb_write(PBSock, 1000, 11000, TestBucket, 2)
        catch
      _:_ ->
          lager:info("Shutdown timeout caught"),
          []
      end,
    lager:info("Received ~p write failures", [length(WriteErrors)]),
    timer:sleep(3000),
    lager:info("Checking number of read failures on secondary cluster"),
    ReadErrors = rt:systest_read(BFirst, 1000, 11000, TestBucket, 2),
    lager:info("Received ~p read failures", [length(ReadErrors)]),

    %% Ensure node is down before we try to start it up again.
    lager:info("pb_write_during_shutdown: Ensure node ~p is down before restart", [Target]),
    ?assertEqual(ok, rt:wait_until_unpingable(Target)),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    rt:wait_for_service(Target, riak_repl),
    ReadErrors2 = rt:systest_read(Target, 1000, 11000, TestBucket, 2),
    lager:info("Received ~p read failures on ~p", [length(ReadErrors2), Target]),
    case length(WriteErrors) >= length(ReadErrors) of
        true ->
            ok;
        false ->
            lager:error("Received more read errors on ~p: ~p than write errors on ~p: ~p",
            [BFirst, length(ReadErrors), Target, length(WriteErrors)]),
            FailedKeys = lists:foldl(fun({Key, _}, Acc) ->
            case lists:keyfind(Key, 1, WriteErrors) of
                false ->
                    [Key|Acc];
                _ ->
                    Acc
            end
        end, [], ReadErrors),
    lager:info("Failed keys ~p", [FailedKeys]),
    ?assert(false)
    end.


%% @doc HTTP Write during Shutdown
%%      Test Cycle:
%%        Connect to Cluster "A" via HTTP
%%        Spawn background process to stop Cluster "A" nodes
%%        Write 10,000 keys to Cluster "A"
%%        Verify that there are and equal number of write failures on Cluster "A" and read failures on Cluster "B"
http_write_during_shutdown([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
    
    TestHash = list_to_binary([io_lib:format("~2.16.0b", [X]) || <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-http_write_shutdown_test">>,

    log_to_nodes(ANodes ++ BNodes, "Testing http writes during shutdown"),

    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
    Target = hd(ANodes -- [LeaderA]),

    ConnInfo = proplists:get_value(Target, rt:connection_info([Target])),
    {IP, Port} = proplists:get_value(http, ConnInfo),
    lager:info("Connecting to http socket ~p:~p on ~p", [IP, Port, Target]),
    C = rt:httpc(Target),

    %% do the stop in the background while we're writing keys
    spawn(fun() ->
              timer:sleep(500),
              lager:info("Stopping node ~p again", [Target]),
              rt:stop(Target),
              lager:info("Node stopped")
          end),

    lager:info("Writing 10,000 keys"),
    WriteErrors =
        try
            http_write(C, 12000, 22000, TestBucket, 2)
        catch
            _:_ ->
                lager:info("Shutdown timeout caught"),
                []
        end,
    lager:info("got ~p write failures to ~p", [length(WriteErrors), Target]),
    timer:sleep(3000),
    lager:info("Checking number of read failures on secondary cluster node, ~p", [BFirst]),
    [{_, {IP, Port2}},_] = rt:connection_info(BFirst),
    C2 = rhc:create("127.0.0.1", Port2, "riak", []),
    ReadErrors = http_read(C2, 12000, 22000, TestBucket, 2),
    lager:info("Received ~p read failures from ~p", [length(ReadErrors), BFirst]),

    %% Ensure node is down before we try to start it up again.
    lager:info("HTTP: write_during_shutdown: Ensure node ~p is down before restart", [Target]),
    ?assertEqual(ok, rt:wait_until_unpingable(Target)),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    rt:wait_for_service(Target, riak_repl),
    ReadErrors2 = http_read(C, 12000, 22000, TestBucket, 2),
    lager:info("Received ~p read failures on ~p", [length(ReadErrors2), Target]),
    case length(WriteErrors) >= length(ReadErrors) of
        true ->
            ok;
        false ->
            lager:error("Received more read errors on ~p: ~p than write errors on ~p: ~p",
            [BFirst, length(ReadErrors), Target, length(WriteErrors)]),
            FailedKeys = lists:foldl(fun({Key, _}, Acc) ->
                case lists:keyfind(Key, 1, WriteErrors) of
                    false ->
                        [Key|Acc];
                    _ ->
                        Acc
                end
            end, [], ReadErrors),
        lager:info("Failed keys ~p", [FailedKeys]),
        ?assert(false)
    end.


client_iterate(_Sock, [], _Bucket, _W, Acc, _Fun, Parent) ->
    Parent ! {result, self(), Acc},
    Acc;


client_iterate(Sock, [N | NS], Bucket, W, Acc, Fun, Parent) ->
    NewAcc = try Fun(Sock, Bucket, N, W) of
        ok ->
            Acc;
        Other ->
            [{N, Other} | Acc]
    catch
        What:Why ->
            [{N, {What, Why}} | Acc]
    end,
    client_iterate(Sock, NS, Bucket, W, NewAcc, Fun, Parent).


http_write(Sock, Start, End, Bucket, W) ->
    F = fun(S, B, K, WVal) ->
            X = list_to_binary(integer_to_list(K)),
            Obj = riakc_obj:new(B, X, X),
            rhc:put(S, Obj, [{dw, WVal}])
    end,
    Keys = lists:seq(Start, End),
    Partitions = partition_keys(Keys, 8),
    Parent = self(),
    Workers = [spawn_monitor(fun() -> client_iterate(Sock, K, Bucket, W, [], F, Parent) end) || K <- Partitions],
    collect_results(Workers, []).


pb_write(Sock, Start, End, Bucket, W) ->
    F = fun(S, B, K, WVal) ->
        Obj = riakc_obj:new(B, <<K:32/integer>>, <<K:32/integer>>),
        riakc_pb_socket:put(S, Obj, [{dw, WVal}])
    end,
    Keys = lists:seq(Start, End),
    Partitions = partition_keys(Keys, 8),
    Parent = self(),
    Workers = [spawn_monitor(fun() -> client_iterate(Sock, K, Bucket, W, [], F, Parent) end) || K <- Partitions],
    collect_results(Workers, []).


http_read(Sock, Start, End, Bucket, R) ->
    F = fun(S, B, K, RVal) ->
        X = list_to_binary(integer_to_list(K)),
        case rhc:get(S, B, X, [{r, RVal}]) of
            {ok, _} ->
                ok;
            Error ->
                Error
        end
    end,
    client_iterate(Sock, lists:seq(Start, End), Bucket, R, [], F, self()).


partition_keys(Keys, PC) ->
    partition_keys(Keys, PC, lists:duplicate(PC, [])).


partition_keys([] , _, Acc) ->
    Acc;


partition_keys(Keys, PC, Acc) ->
    In = lists:sublist(Keys, PC),
    Rest = try lists:nthtail(PC, Keys)
        catch _:_ -> []
    end,
    NewAcc = lists:foldl(fun(K, [H|T]) ->
        T ++ [[K|H]]
        end, Acc, In),
    partition_keys(Rest, PC, NewAcc).


collect_results([], Acc) ->
    Acc;


collect_results(Workers, Acc) ->
    receive
        {result, Pid, Res} ->
            collect_results(lists:keydelete(Pid, 1, Workers), Res ++ Acc);
        {'DOWN', _, _, Pid, _Reason} ->
            collect_results(lists:keydelete(Pid, 1, Workers), Acc)
    end.
