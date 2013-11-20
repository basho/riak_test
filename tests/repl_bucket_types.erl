%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(repl_bucket_types).
-behaviour(riak_test).
-export([confirm/0, verify_rt/2]).
-include_lib("eunit/include/eunit.hrl").

-define(RTSINK_MAX_WORKERS, 1).
-define(RTSINK_MAX_PENDING, 1).

%% Replication Bucket Types test
%%

%% @doc riak_test entry point
confirm() ->

    rt:set_conf(all, [{"buckets.default.siblings", "off"}]),
    %% Start up two >1.3.2 clusters and connect them,
    {LeaderA, LeaderB, ANodes, BNodes} = make_clusters(),

    %%rpc:multicall([LeaderA, LeaderB], lager, set_loglevel, [lager_console_backend, debug]),
    rpc:multicall([LeaderA, LeaderB], app_helper, set_env, [riak_repl, true]),

    lager:info("creating typed bucket on ~p", [LeaderA]),

    {ok, [{"127.0.0.1", PortA}]} = rpc:call(LeaderA, application, get_env,
                                           [riak_api, pb]),

    {ok, PBA} = riakc_pb_socket:start_link("127.0.0.1", PortA, []),

    {ok, [{"127.0.0.1", PortB}]} = rpc:call(LeaderB, application, get_env,
                                           [riak_api, pb]),

    {ok, PBB} = riakc_pb_socket:start_link("127.0.0.1", PortB, []),

    Type = <<"firsttype">>,
    rt:create_and_activate_bucket_type(LeaderA, Type, [{n_val, 3}]),
    rt:wait_until_bucket_type_status(Type, active, ANodes),

    rt:create_and_activate_bucket_type(LeaderB, Type, [{n_val, 3}]),
    rt:wait_until_bucket_type_status(Type, active, BNodes),

    UndefType = <<"undefinedtype">>,
    rt:create_and_activate_bucket_type(LeaderA, UndefType, [{n_val, 3}]),
    rt:wait_until_bucket_type_status(UndefType, active, ANodes),

    connect_clusters(LeaderA, LeaderB),

    %% Enable RT replication from cluster "A" to cluster "B"
    lager:info("Enabling realtime between ~p and ~p", [LeaderA, LeaderB]),
    enable_rt(LeaderA, ANodes),

    %%verify_rt(LeaderA, LeaderB),

    lager:info("doing untyped put on A"),
    Bin = <<"data data data">>,
    Key = <<"key">>,
    Bucket = <<"kicked">>,
    Obj = riakc_obj:new(Bucket, Key, Bin),
    riakc_pb_socket:put(PBA, Obj, [{w,3}]),
    
    lager:info("waiting for untyped pb get on B"),

    F = fun() ->
       case riakc_pb_socket:get(PBB, <<"kicked">>, <<"key">>) of
        {ok, O6} ->
            lager:info("Got result from untyped get on B"),          
          ?assertEqual(<<"data data data">>, riakc_obj:get_value(O6)),
           true;
       _ ->
           lager:info("No result from untyped get on B, trying again..."),          
           false
       end
    end,

    rt:wait_until(F),

    lager:info("doing typed put on A"),
    Type = <<"firsttype">>,
    BucketTyped = {Type, <<"typekicked">>},
    KeyTyped = <<"keytyped">>,
    ObjTyped = riakc_obj:new(BucketTyped, KeyTyped, Bin),
    lager:info("bucket type of object:~p", [riakc_obj:bucket_type(ObjTyped)]),
    riakc_pb_socket:put(PBA, ObjTyped, [{w,3}]),
    lager:info("waiting for typed pb get on B"),

    FType = fun() ->
       case riakc_pb_socket:get(PBB, {Type, <<"typekicked">>}, <<"keytyped">>) of
        {ok, O6} ->
            lager:info("Got result from typed get on B"),          
            ?assertEqual(<<"data data data">>, riakc_obj:get_value(O6)),
            true;
        _ ->
            lager:info("No result from untyped get on B, trying again..."),          
            false
        end
    end,

    rt:wait_until(FType),

    lager:info("doing typed put on A where type has not been defined"),
    UndefBucketTyped = {UndefType, <<"badtype">>},
    UndefKeyTyped = <<"badkeytyped">>,
    UndefObjTyped = riakc_obj:new(UndefBucketTyped, UndefKeyTyped, Bin),
    lager:info("bucket type of object:~p", [riakc_obj:bucket_type(UndefObjTyped)]),
    riakc_pb_socket:put(PBA, UndefObjTyped, [{w,3}]),

    lager:info("waiting for undefined type pb get on B, there should be no response"),

    FUndefType = fun() ->
       case riakc_pb_socket:get(PBB, UndefBucketTyped, UndefKeyTyped) of
        {ok, Res} ->
            lager:info("Got result from get on B"),          
            ?assertEqual(<<"data data data">>, riakc_obj:get_value(Res)),
            false;
        _ ->
            lager:info("No result from untyped get on B, trying again..."),          
            true
        end
    end,

    rt:wait_until(FUndefType),

    lager:info("Undefined type object not replicated, correct response."),

    pass.

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes).

%% @doc Verify that RealTime replication is functioning correctly by
%%     writing some objects to cluster A and checking they can be
%%     read from cluster B. Each call creates a new bucket so that
%%     verification can be tested multiple times independently.
verify_rt(LeaderA, LeaderB) ->
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,
    First = 101,
    Last = 200,

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
              {fullsync_interval, disabled},
              {rtq_max_bytes, 1048576}
              %%{rtsink_max_workers, ?RTSINK_MAX_WORKERS},
              %%{rt_heartbeat_timeout, ?RTSINK_MAX_PENDING}
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
    %% connect_clusters(AFirst, BFirst),

    {AFirst, BFirst, ANodes, BNodes}.
