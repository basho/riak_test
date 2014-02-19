%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(repl_consistent_object_filter).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% Test to verify that replication properly filters consistent bucket
%% types. This is intended to be a temporary state of affairs so this
%% test should have a limited life span.
%%
%% Currently this test only exercises fullsync replication. The write
%% path for consistent objects bypasses the riak_kv postcommit hooks
%% that are the mechanism by which realtime replication works. As a
%% result, no attempt is ever made to replicate consistent objects.

%% @doc riak_test entry point
confirm() ->
    rt:set_conf(all, [{"buckets.default.siblings", "off"}]),
    rt:set_conf(all, [{"strong_consistency", "on"}]),

    %% Start up two >1.3.2 clusters and connect them,
    {LeaderA, LeaderB, ANodes, BNodes} = make_clusters(),

    %% Temporary kludge to handle startup race condition between
    %% riak_kv and riak_ensemble
    %% @TODO Remove this once riak_ensemble helpers are in place that
    %% provide a way for this race to be resolved.
    timer:sleep(60000),

    PBA = get_pb_pid(LeaderA),
    PBB = get_pb_pid(LeaderB),

    BucketType = <<"consistent_type">>,

    %% Create consistent bucket type on cluster A
    rt:create_and_activate_bucket_type(LeaderA, BucketType, [{consistent, true}]),
    rt:wait_until_bucket_type_status(BucketType, active, ANodes),

    %% Create consistent bucket type on cluster B
    rt:create_and_activate_bucket_type(LeaderB, BucketType, [{consistent, true}]),
    rt:wait_until_bucket_type_status(BucketType, active, BNodes),

    connect_clusters(LeaderA, LeaderB),

    %% Create two riak objects and execute consistent put of those
    %% objects
    Bucket = <<"unclebucket">>,
    Key1 = <<"Maizy">>,
    Key2 = <<"Miles">>,
    Bin1 = <<"Take this quarter, go downtown, and have a rat gnaw that thing off your face! Good day to you, madam.">>,
    Bin2 = <<"My Uncle was micro waving our socks and the dog threw up on the couch for an hour.">>,
    Obj1 = riakc_obj:new({BucketType, Bucket}, Key1, Bin1),
    Obj2 = riakc_obj:new({BucketType, Bucket}, Key2, Bin2),
    lager:info("doing 2 consistent puts on A, bucket:~p", [Bucket]),
    ok = riakc_pb_socket:put(PBA, Obj1),
    ok = riakc_pb_socket:put(PBA, Obj2),

    %% Enable fullsync and wait for it to complete
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    {Time, _} = timer:tc(repl_util, start_and_wait_until_fullsync_complete, [LeaderA]),
    lager:info("Fullsync completed in ~p seconds", [Time/1000/1000]),

    %% Attempt to read the objects from cluster B to verify they have
    %% not been replicated via realtime replication
    BReadRes3 = riakc_pb_socket:get(PBB, {BucketType, Bucket}, Key1),
    BReadRes4 = riakc_pb_socket:get(PBB, {BucketType, Bucket}, Key2),

    ?assertEqual({error, notfound}, BReadRes3),
    ?assertEqual({error, notfound}, BReadRes4),

    riakc_pb_socket:stop(PBA),
    riakc_pb_socket:stop(PBB),
    pass.

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

%% @doc Create two clusters of 1 node each and connect them for replication:
%%      Cluster "A" -> cluster "B"
make_clusters() ->
    NumNodes = rt_config:get(num_nodes, 2),
    ClusterASize = rt_config:get(cluster_a_size, 1),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {max_fssource_node, 2},
              {max_fssink_node, 2},
              {max_fssource_cluster, 5},
              {max_fssource_retries, 5}
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

    repl_util:wait_until_leader_converge(ANodes),
    repl_util:wait_until_leader_converge(BNodes),

    {AFirst, BFirst, ANodes, BNodes}.

get_pb_pid(Leader) ->
    {ok, [{IP, PortA}] } = rpc:call(Leader, application, get_env, [riak_api, pb]),
    {ok, Pid} = riakc_pb_socket:start_link(IP, PortA, []),
    Pid.
