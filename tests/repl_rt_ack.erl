%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(repl_rt_ack).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(RPC_TIMEOUT, 5000).
-define(HB_TIMEOUT,  2000).

%% Replication Realtime Ack test
%%
%%
%% Test:
%% -----
%% Start up two >1.3.2 clusters and connect them,
%% Enable RT replication,
%% Write one objects to the source cluster (A),
%% Verify they got to the sink cluster (B),
%% Verify that the unack stat is not incremented

%% @doc riak_test entry point
confirm() ->
    %% Start up two >1.3.2 clusters and connect them,
    {LeaderA, LeaderB, ANodes, _BNodes} = make_connected_clusters(),

    %% Enable RT replication from cluster "A" to cluster "B"
    enable_rt(LeaderA, ANodes),

    %% Verify that heartbeats are being acknowledged by the sink (B) back to source (A)
    %?assertEqual(verify_heartbeat_messages(LeaderA), true),

    %% Verify RT repl of objects
    verify_rt(LeaderA, LeaderB, 1),

    RTQStatus = rpc:call(LeaderA, riak_repl2_rtq, status, []),

    Consumers = proplists:get_value(consumers, RTQStatus),
    case proplists:get_value("B", Consumers) of
                 undefined ->
                    [];
                 Consumer ->
                    Unacked = proplists:get_value(unacked, Consumer, 0),
                    lager:info("unacked: ~p", [Unacked]),
                    ?assertEqual(0, Unacked)
    end,


    pass.

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes).

%% @doc Verify that RealTime replication is functioning correctly by
%%      writing some objects to cluster A and checking they can be
%%      read from cluster B. Each call creates a new bucket so that
%%      verification can be tested multiple times independently.
verify_rt(LeaderA, LeaderB, NumMessages) ->
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,
    First = 1,
    Last = NumMessages,

    %% Write some objects to the source cluster (A),
    lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
               [Last-First+1, LeaderA, LeaderB]),
    ?assertEqual([], repl_util:do_write(LeaderA, First, Last, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading ~p keys written from ~p", [Last-First+1, LeaderB]),
    ?assertEqual(0, repl_util:wait_for_reads(LeaderB, First, Last, TestBucket, 2)).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

%% @doc Create two clusters of 3 nodes each and connect them for replication:
%%      Cluster "A" -> cluster "B"
make_connected_clusters() ->
    NumNodes = rt_config:get(num_nodes, 4),
    ClusterASize = rt_config:get(cluster_a_size, 2),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled}
             ]},
            {yokozuna, 
            [{enabled, false}
            ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    %% get the leader for the first cluster
    lager:info("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    %% get the leader for the second cluster
    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    %% Name the clusters
    repl_util:name_cluster(AFirst, "A"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(BNodes),

    %% Connect for replication
    connect_clusters(AFirst, BFirst),

    {AFirst, BFirst, ANodes, BNodes}.
% end
