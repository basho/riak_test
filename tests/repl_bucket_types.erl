%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(repl_bucket_types).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(ENSURE_READ_ITERATIONS, 5).
-define(ENSURE_READ_INTERVAL, 1000).

%% Replication Bucket Types test
%%

%% @doc riak_test entry point
confirm() ->

    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    %% Start up two >1.3.2 clusters and connect them,
    {LeaderA, LeaderB, ANodes, BNodes} = ClusterNodes = make_clusters(),

    PBA = get_pb_pid(LeaderA),
    PBB = get_pb_pid(LeaderB),

    {DefinedType,  UndefType} = Types = {<<"working_type">>, <<"undefined_type">>},

    rt:create_and_activate_bucket_type(LeaderA, DefinedType, [{n_val, 3}, {allow_mult, false}]),
    rt:wait_until_bucket_type_status(DefinedType, active, ANodes),

    rt:create_and_activate_bucket_type(LeaderB, DefinedType, [{n_val, 3}, {allow_mult, false}]),
    rt:wait_until_bucket_type_status(DefinedType, active, BNodes),

    rt:create_and_activate_bucket_type(LeaderA, UndefType, [{n_val, 3}, {allow_mult, false}]),
    rt:wait_until_bucket_type_status(UndefType, active, ANodes),

    connect_clusters(LeaderA, LeaderB),

    realtime_test(ClusterNodes, Types, PBA, PBB),
    fullsync_test(ClusterNodes, Types, PBA, PBB),

    riakc_pb_socket:stop(PBA),
    riakc_pb_socket:stop(PBB),

    rt:clean_cluster(ANodes ++ BNodes),

    {MixedLeaderA, MixedLeaderB, MixedANodes, _MixedBNodes} = MixedClusterNodes = make_mixed_clusters(),

    rt:create_and_activate_bucket_type(MixedLeaderA, DefinedType, [{n_val, 3}, {allow_mult, false}]),
    rt:wait_until_bucket_type_status(DefinedType, active, MixedANodes),

    DPBA = get_pb_pid(MixedLeaderA),
    DPBB = get_pb_pid(MixedLeaderB),

    connect_clusters(LeaderA, LeaderB),

    realtime_mixed_version_test(MixedClusterNodes, Types, DPBA, DPBB),
    fullsync_mixed_version_test(MixedClusterNodes, Types, DPBA, DPBB),

    riakc_pb_socket:stop(DPBA),
    riakc_pb_socket:stop(DPBB),
    pass.

realtime_test(ClusterNodes, BucketTypes, PBA, PBB) ->
    {LeaderA, LeaderB, ANodes, BNodes} = ClusterNodes,
    {DefinedType, UndefType} = BucketTypes,

    %% Enable RT replication from cluster "A" to cluster "B"
    lager:info("Enabling realtime between ~p and ~p", [LeaderA, LeaderB]),
    enable_rt(LeaderA, ANodes),

    Bin = <<"data data data">>,
    Key = <<"key">>,
    Bucket = <<"kicked">>,
    DefaultObj = riakc_obj:new(Bucket, Key, Bin),
    lager:info("doing untyped put on A, bucket:~p", [Bucket]),
    riakc_pb_socket:put(PBA, DefaultObj, [{w,3}]),

    UntypedWait = make_pbget_fun(PBB, Bucket, Key, Bin),
    ?assertEqual(ok, rt:wait_until(UntypedWait)),

    BucketTyped = {DefinedType, <<"typekicked">>},
    KeyTyped = <<"keytyped">>,
    ObjTyped = riakc_obj:new(BucketTyped, KeyTyped, Bin),

    lager:info("doing typed put on A, bucket:~p", [BucketTyped]),
    riakc_pb_socket:put(PBA, ObjTyped, [{w,3}]),

    TypedWait = make_pbget_fun(PBB, BucketTyped, KeyTyped, Bin),
    ?assertEqual(ok, rt:wait_until(TypedWait)),

    UndefBucketTyped = {UndefType, <<"badtype">>},
    UndefKeyTyped = <<"badkeytyped">>,
    UndefObjTyped = riakc_obj:new(UndefBucketTyped, UndefKeyTyped, Bin),

    lager:info("doing typed put on A where type is not defined on B, bucket:~p", [UndefBucketTyped]),
    riakc_pb_socket:put(PBA, UndefObjTyped, [{w,3}]),

    lager:info("waiting for undefined type pb get on B, should get error <<\"no_type\">>"),

    ErrorResult = riakc_pb_socket:get(PBB, UndefBucketTyped, UndefKeyTyped),
    ?assertEqual({error, <<"no_type">>}, ErrorResult),

    DefaultProps = get_current_bucket_props(BNodes, DefinedType),
    ?assertEqual({n_val, 3}, lists:keyfind(n_val, 1, DefaultProps)),

    UpdatedProps = update_props(DefinedType,
                                [{n_val, 1}],
                                LeaderB,
                                BNodes),
    ?assertEqual({n_val, 1}, lists:keyfind(n_val, 1, UpdatedProps)),

    UnequalObjBin = <<"unequal props val">>,
    UnequalPropsObj = riakc_obj:new(BucketTyped, KeyTyped, UnequalObjBin),
    lager:info("doing put of typed bucket on A where bucket properties "
               "(n_val 3 versus n_val 1) are not equal on B"),
    riakc_pb_socket:put(PBA, UnequalPropsObj, [{w,3}]),

    lager:info("checking to ensure the bucket contents were not updated."),
    ensure_bucket_not_updated(PBB, BucketTyped, KeyTyped, Bin),
    disable_rt(LeaderA, ANodes),

    UpdatedProps2 = update_props(DefinedType,
                                 [{n_val, 3}],
                                 LeaderB,
                                 BNodes),
    ?assertEqual({n_val, 3}, lists:keyfind(n_val, 1, UpdatedProps2)),
    ?assertEqual({n_val, 3}, lists:keyfind(n_val, 1, UpdatedProps2)),
    disable_rt(LeaderA, ANodes).

realtime_mixed_version_test(ClusterNodes, BucketTypes, PBA, PBB) ->
    {LeaderA, LeaderB, ANodes, _BNodes} = ClusterNodes,
    {DefinedType, _UndefType} = BucketTypes,

    %% Enable RT replication from cluster "A" to cluster "B"
    lager:info("Enabling realtime between ~p and ~p", [LeaderA, LeaderB]),
    enable_rt(LeaderA, ANodes),

    Bin = <<"data data data">>,
    Key = <<"key">>,
    Bucket = <<"kicked">>,
    DefaultObj = riakc_obj:new(Bucket, Key, Bin),
    lager:info("doing untyped put on A, bucket:~p", [Bucket]),
    riakc_pb_socket:put(PBA, DefaultObj, [{w,3}]),

    %% make sure we rt replicate a "default" type bucket
    UntypedWait = make_pbget_fun(PBB, Bucket, Key, Bin),
    ?assertEqual(ok, rt:wait_until(UntypedWait)),

    DowngradedBucketTyped = {DefinedType, <<"typekicked">>},
    KeyTyped = <<"keytyped">>,
    ObjTyped = riakc_obj:new(DowngradedBucketTyped, KeyTyped, Bin),

    lager:info("doing typed put on A with downgraded B, bucket:~p", [DowngradedBucketTyped]),
    riakc_pb_socket:put(PBA, ObjTyped, [{w,3}]),

    lager:info("checking to ensure the bucket contents were not sent to previous version B."),
    ensure_bucket_not_sent(PBB, DowngradedBucketTyped, KeyTyped).

fullsync_test(ClusterNodes, BucketTypes, PBA, PBB) ->
    {LeaderA, LeaderB, ANodes, BNodes} = ClusterNodes,
    {DefinedType, UndefType} = BucketTypes,

    %% Enable RT replication from cluster "A" to cluster "B"
    lager:info("Enabling fullsync between ~p and ~p", [LeaderA, LeaderB]),
    enable_fullsync(LeaderA, ANodes),

    Bin = <<"data data data">>,
    Key = <<"key">>,
    Bucket = <<"fullsync-kicked">>,
    DefaultObj = riakc_obj:new(Bucket, Key, Bin),
    lager:info("doing untyped put on A, bucket:~p", [Bucket]),
    riakc_pb_socket:put(PBA, DefaultObj, [{w,3}]),

    BucketTyped = {DefinedType, <<"fullsync-typekicked">>},
    KeyTyped = <<"keytyped">>,
    ObjTyped = riakc_obj:new(BucketTyped, KeyTyped, Bin),

    lager:info("doing typed put on A, bucket:~p", [BucketTyped]),
    riakc_pb_socket:put(PBA, ObjTyped, [{w,3}]),

    UndefBucketTyped = {UndefType, <<"fullsync-badtype">>},
    UndefKeyTyped = <<"badkeytyped">>,
    UndefObjTyped = riakc_obj:new(UndefBucketTyped, UndefKeyTyped, Bin),

    lager:info("doing typed put on A where type is not "
               "defined on B, bucket:~p",
               [UndefBucketTyped]),

    riakc_pb_socket:put(PBA, UndefObjTyped, [{w,3}]),

    {SyncTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    lager:info("Fullsync completed in ~p seconds", [SyncTime1/1000/1000]),

    ReadResult1 =  riakc_pb_socket:get(PBB, Bucket, Key),
    ReadResult2 =  riakc_pb_socket:get(PBB, BucketTyped, KeyTyped),
    ReadResult3 =  riakc_pb_socket:get(PBB, UndefBucketTyped, UndefKeyTyped),

    ?assertMatch({ok, _}, ReadResult1),
    ?assertMatch({ok, _}, ReadResult2),
    ?assertMatch({error, _}, ReadResult3),

    {ok, ReadObj1} = ReadResult1,
    {ok, ReadObj2} = ReadResult2,

    ?assertEqual(Bin, riakc_obj:get_value(ReadObj1)),
    ?assertEqual(Bin, riakc_obj:get_value(ReadObj2)),
    ?assertEqual({error, <<"no_type">>}, ReadResult3),

    DefaultProps = get_current_bucket_props(BNodes, DefinedType),
    ?assertEqual({n_val, 3}, lists:keyfind(n_val, 1, DefaultProps)),

    UpdatedProps = update_props(DefinedType,  [{n_val, 1}], LeaderB, BNodes),
    ?assertEqual({n_val, 1}, lists:keyfind(n_val, 1, UpdatedProps)),

    UnequalObjBin = <<"unequal props val">>,
    UnequalPropsObj = riakc_obj:new(BucketTyped, KeyTyped, UnequalObjBin),
    lager:info("doing put of typed bucket on A where bucket properties (n_val 3 versus n_val 1) are not equal on B"),
    riakc_pb_socket:put(PBA, UnequalPropsObj, [{w,3}]),

    {SyncTime2, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    lager:info("Fullsync completed in ~p seconds", [SyncTime2/1000/1000]),

    lager:info("checking to ensure the bucket contents were not updated."),
    ensure_bucket_not_updated(PBB, BucketTyped, KeyTyped, Bin).

fullsync_mixed_version_test(ClusterNodes, BucketTypes, PBA, PBB) ->
    {LeaderA, LeaderB, ANodes, _BNodes} = ClusterNodes,
    {DefinedType, _UndefType} = BucketTypes,

    %% Enable RT replication from cluster "A" to cluster "B"
    lager:info("Enabling fullsync between ~p and ~p", [LeaderA, LeaderB]),
    enable_fullsync(LeaderA, ANodes),

    Bin = <<"data data data">>,
    Key = <<"key">>,
    Bucket = <<"fullsync-kicked">>,
    DefaultObj = riakc_obj:new(Bucket, Key, Bin),
    lager:info("doing untyped put on A, bucket:~p", [Bucket]),
    riakc_pb_socket:put(PBA, DefaultObj, [{w,3}]),

    BucketTyped = {DefinedType, <<"fullsync-typekicked">>},
    KeyTyped = <<"keytyped">>,
    ObjTyped = riakc_obj:new(BucketTyped, KeyTyped, Bin),

    lager:info("doing typed put on A, bucket:~p", [BucketTyped]),
    riakc_pb_socket:put(PBA, ObjTyped, [{w,3}]),

    {SyncTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    lager:info("Fullsync completed in ~p seconds", [SyncTime1/1000/1000]),

    ReadResult1 = riakc_pb_socket:get(PBB, Bucket, Key),
    ReadResult2 = riakc_pb_socket:get(PBB, BucketTyped, KeyTyped),

    ?assertMatch({ok, _}, ReadResult1),
    ?assertMatch({error, _}, ReadResult2).
  
%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    repl_util:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes).

%% @doc Turn off Realtime replication on the cluster lead by LeaderA.
disable_rt(LeaderA, ANodes) ->
    repl_util:disable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    repl_util:stop_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes).

%% @doc Turn on fullsync replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_fullsync(LeaderA, ANodes) ->
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes).


%% @doc Turn ff fullsync replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
%disable_fullsync(LeaderA, ANodes) ->
%    repl_util:disable_fullsync(LeaderA, "B"),
%    rt:wait_until_ring_converged(ANodes).

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
              {max_fssource_cluster, 20},
              {max_fssource_node, 20},
              {max_fssink_node, 20},
              {rtq_max_bytes, 1048576}
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

    {AFirst, BFirst, ANodes, BNodes}.

%% @doc Create two clusters of 1 node each and connect them for replication:
%%      Cluster "A" -> cluster "B"
make_mixed_clusters() ->
    NumNodes = rt_config:get(num_nodes, 2),
    ClusterASize = rt_config:get(cluster_a_size, 1),

    lager:info("Deploy ~p mixed version nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              {max_fssource_cluster, 20},
              {max_fssource_node, 20},
              {max_fssink_node, 20},
              {rtq_max_bytes, 1048576}
             ]}
           ],
    MixedConf = [{current, Conf}, {previous, Conf}],
    Nodes = rt:deploy_nodes(MixedConf),
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

    {AFirst, BFirst, ANodes, BNodes}.

make_pbget_fun(Pid, Bucket, Key, Bin) ->
    fun() ->
            case riakc_pb_socket:get(Pid, Bucket, Key) of
                {ok, O6} ->
                    ?assertEqual(Bin, riakc_obj:get_value(O6)),
                    true;
                _ ->
                    false
            end
    end.

ensure_bucket_not_sent(Pid, Bucket, Key) ->
    Results = [ assert_bucket_not_found(Pid, Bucket, Key) || _I <- lists:seq(1, ?ENSURE_READ_ITERATIONS)],
    ?assertEqual(false, lists:member(false, Results)).

ensure_bucket_not_updated(Pid, Bucket, Key, Bin) ->
    Results = [ value_unchanged(Pid, Bucket, Key, Bin) || _I <- lists:seq(1, ?ENSURE_READ_ITERATIONS)],
    ?assertEqual(false, lists:member(false, Results)).

value_unchanged(Pid, Bucket, Key, Bin) ->
    case riakc_pb_socket:get(Pid, Bucket, Key) of
        {error, E} ->
            lager:info("Got error:~p from get on cluster B", [E]),
            false;
        {ok, Res} ->
            ?assertEqual(Bin, riakc_obj:get_value(Res)),
            true
    end,
    timer:sleep(?ENSURE_READ_INTERVAL).


assert_bucket_not_found(Pid, Bucket, Key) ->
    case riakc_pb_socket:get(Pid, Bucket, Key) of
        {error, notfound} ->
	    true;
	{ok, Res} ->
            lager:error("Found bucket:~p and key:~p on sink when we should not have", [Res, Key]),
	    false
    end.

get_pb_pid(Leader) ->
    {ok, [{IP, PortA}] } = rpc:call(Leader, application, get_env, [riak_api, pb]),
    {ok, Pid} = riakc_pb_socket:start_link(IP, PortA, []),
    Pid.

update_props(Type, Updates, Node, Nodes) ->
    lager:info("Setting bucket properties ~p for bucket type ~p on node ~p",
               [Updates, Type, Node]),
    rpc:call(Node, riak_core_bucket_type, update, [Type, Updates]),
    rt:wait_until_ring_converged(Nodes),

    get_current_bucket_props(Nodes, Type).

%% fetch bucket properties via rpc
%% from a node or a list of nodes (one node is chosen at random)
get_current_bucket_props(Nodes, Type) when is_list(Nodes) ->
    Node = lists:nth(length(Nodes), Nodes),
    get_current_bucket_props(Node, Type);
get_current_bucket_props(Node, Type) when is_atom(Node) ->
    rpc:call(Node,
             riak_core_bucket_type,
             get,
             [Type]).
