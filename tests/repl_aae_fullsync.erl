%% @doc
%% This module implements a riak_test to exercise the Active Anti-Entropy Fullsync replication.
%% It sets up two clusters and starts a single fullsync worker for a single AAE tree.
-module(repl_aae_fullsync).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             log_to_nodes/2,
             log_to_nodes/3,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

confirm() ->
    NumNodes = rt:config(num_nodes, 6),
    ClusterASize = rt:config(cluster_a_size, 3),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_kv,
                [
                 %% TODO: confirm AAE options with Joe
                 {anti_entropy, {on, []}},
                 {anti_entropy_build_limit, {100, 1000}},
                 {anti_entropy_concurrency, 100}
                ]
            },
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval, disabled}
             ]}
           ],

    Nodes = deploy_nodes(NumNodes, Conf),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    aae_fs_test(ANodes, BNodes, false),
    pass.

aae_fs_test([AFirst|_] = ANodes, [BFirst|_] = BNodes, Connected) ->

    AllNodes = ANodes ++ BNodes,
    log_to_nodes(AllNodes, "Starting AAE Fullsync test"),

    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-systest_a">>,

    case Connected of
        false ->
            %% clusters are not connected, connect them

            repl_util:name_cluster(AFirst, "A"),
            repl_util:name_cluster(BFirst, "B"),

            %% we'll need to wait for cluster names before continuing
            rt:wait_until_ring_converged(ANodes),
            rt:wait_until_ring_converged(BNodes),

            lager:info("waiting for leader to converge on cluster A"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
            lager:info("waiting for leader to converge on cluster B"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

            {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                [riak_core, cluster_mgr]),

            lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
            repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
            ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B"));

        _ ->
            lager:info("clusters should already be connected"),
            lager:info("waiting for leader to converge on cluster A"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
            lager:info("waiting for leader to converge on cluster B"),
            ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
            lager:info("Leader on cluster A is ~p", [LeaderA]),
            lager:info("BFirst on cluster B is ~p", [BFirst]),
            {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                [riak_core, cluster_mgr]),
            lager:info("B is ~p with port ~p", [BFirst, Port])
    end,

    %% make sure we are connected
    lager:info("Wait for cluster connection A:~p -> B:~p:~p", [LeaderA, BFirst, Port]),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %%---------------------------------------------------
    %% TEST: write data, NOT replicated by RT or fullsync
    %% keys: 1-100
    %%---------------------------------------------------
    lager:info("Writing 100 keys to ~p", [AFirst]),
    ?assertEqual([], repl_util:do_write(AFirst, 1, 100, TestBucket, 2)),

    %% check that the keys we wrote initially aren't replicated yet, because
    %% we've disabled fullsync_on_connect
    lager:info("Check keys written before repl was connected are not present"),
    Res2 = rt:systest_read(BFirst, 1, 100, TestBucket, 2),
    ?assertEqual(100, length(Res2)),

    %%-----------------------------------
    %% TEST: write data, replicated by RT
    %% keys: 101-200
    %%-----------------------------------

    %% Enable and start Real-time replication
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    log_to_nodes(AllNodes, "Write data to A, verify replication to B via realtime"),
    %% write some data on A
    %io:format("~p~n", [rpc:call(LeaderA, riak_repl_console, status, [quiet])]),
    lager:info("Writing 100 more keys to ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 101, 200, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 200, TestBucket, 2)),

    %%---------------------------------------------------------
    %% TEST: fullsync, check that non-RT'd keys get repl'd to B
    %% keys: 1-100
    %%---------------------------------------------------------

    %% run AAE fullsync from A -> B
    {ok, {IP, Port2}} = rpc:call(BFirst, application, get_env, [riak_core, cluster_mgr]),
    Partition = 0,
    IndexN = {0,3},
    log_to_nodes(AllNodes, "Starting AAE fullsync source connection from ~p to ~p:~p", [AFirst,IP,Port2]),
    lager:info("Starting AAE fullsync source, connection from ~p to ~p:~p", [AFirst,IP,Port2]),
    {ok, _SyncSource} = rpc:call(AFirst, riak_repl2_fssource, start_link, [{Partition,IndexN}, {IP,Port2}]),
    
    %% wait for fullsync worker to complete
    %% TODO: how?
    timer:sleep(10000),
        
    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 1, 100, TestBucket, 2)),

    ok.

