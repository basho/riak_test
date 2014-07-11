%% @doc
%% This module implements a riak_test to exercise the Active Anti-Entropy Fullsync replication.
%% It sets up two clusters and starts a single fullsync worker for a single AAE tree.
-module(repl_aae_fullsync_util).
-export([make_clusters/3,
        prepare_cluster_data/5]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             log_to_nodes/2,
             log_to_nodes/3,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

make_clusters(NumNodesWanted, ClusterSize, Conf) ->
    NumNodes = rt_config:get(num_nodes, NumNodesWanted),
    ClusterASize = rt_config:get(cluster_a_size, ClusterSize),
    lager:info("Deploy ~p nodes", [NumNodes]),
    Nodes = deploy_nodes(NumNodes, Conf),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),
    {ANodes, BNodes}.

prepare_cluster_data(TestBucket, NumKeysAOnly, _NumKeysBoth, [AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
    AllNodes = ANodes ++ BNodes,
    log_to_nodes(AllNodes, "Starting AAE Fullsync test"),

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
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %% make sure we are connected
    lager:info("Wait for cluster connection A:~p -> B:~p:~p", [LeaderA, BFirst, Port]),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    %%---------------------------------------------------
    %% TEST: write data, NOT replicated by RT or fullsync
    %% keys: 1..NumKeysAOnly
    %%---------------------------------------------------

    lager:info("Writing ~p keys to A(~p)", [NumKeysAOnly, AFirst]),
    ?assertEqual([], repl_util:do_write(AFirst, 1, NumKeysAOnly, TestBucket, 2)),

    %% check that the keys we wrote initially aren't replicated yet, because
    %% we've disabled fullsync_on_connect
    lager:info("Check keys written before repl was connected are not present"),
    Res2 = rt:systest_read(BFirst, 1, NumKeysAOnly, TestBucket, 1, <<>>, true),
    ?assertEqual(NumKeysAOnly, length(Res2)),

    %% wait for the AAE trees to be built so that we don't get a not_built error
    rt:wait_until_aae_trees_built(ANodes),
    rt:wait_until_aae_trees_built(BNodes),
    ok.
