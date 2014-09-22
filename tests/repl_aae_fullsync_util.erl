%% @doc
%% This module implements a riak_test to exercise the Active Anti-Entropy Fullsync replication.
%% It sets up two clusters and starts a single fullsync worker for a single AAE tree.
-module(repl_aae_fullsync_util).
-export([make_clusters/3,
         prepare_cluster/2,
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

prepare_cluster([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
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
    LeaderA.


prepare_cluster_data(TestBucket, NumKeysAOnly, _NumKeysBoth, [AFirst|_] = ANodes, [BFirst|_] = BNodes) ->
    
    prepare_cluster(ANodes, BNodes),
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
