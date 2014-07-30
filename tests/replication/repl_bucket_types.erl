%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(repl_bucket_types).
-behaviour(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(ENSURE_READ_ITERATIONS, 5).
-define(ENSURE_READ_INTERVAL, 1000).

%% Replication Bucket Types test
%%

setup(Type) ->
    rt_config:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    {LeaderA, LeaderB, ANodes, BNodes} = ClusterNodes = make_clusters(Type),

    PBA = rt_pb:pbc(LeaderA),
    PBB = rt_pb:pbc(LeaderB),

    {DefinedType, UndefType} = Types = {<<"working_type">>, <<"undefined_type">>},

    rt:create_and_activate_bucket_type(LeaderA,
                                       DefinedType,
                                       [{n_val, 3}, {allow_mult, false}]),
    rt:wait_until_bucket_type_status(DefinedType, active, ANodes),
    rt:wait_until_bucket_type_visible(ANodes, DefinedType),

    case Type of
        current ->
            rt:create_and_activate_bucket_type(LeaderB,
                                               DefinedType,
                                               [{n_val, 3}, {allow_mult, false}]),
            rt:wait_until_bucket_type_status(DefinedType, active, BNodes),
            rt:wait_until_bucket_type_visible(BNodes, DefinedType);
        mixed ->
            ok
    end,

    rt:create_and_activate_bucket_type(LeaderA,
                                       UndefType,
                                       [{n_val, 3}, {allow_mult, false}]),
    rt:wait_until_bucket_type_status(UndefType, active, ANodes),
    rt:wait_until_bucket_type_visible(ANodes, UndefType),

    connect_clusters(LeaderA, LeaderB),
    {ClusterNodes, Types, PBA, PBB}.

cleanup({ClusterNodes, _Types, PBA, PBB}, CleanCluster) ->
    riakc_pb_socket:stop(PBA),
    riakc_pb_socket:stop(PBB),
    {_, _, ANodes, BNodes} = ClusterNodes,
    case CleanCluster of
        true ->
            rt_cluster:clean_cluster(ANodes ++ BNodes);
        false ->
            ok
    end.

%% @doc riak_test entry point
confirm() ->
    %% Test two clusters of the current version
    SetupData = setup(current),
    realtime_test(SetupData),
    fullsync_test(SetupData),
    cleanup(SetupData, true),

    %% Test a cluster of the current version replicating to a cluster
    %% of the previous version
    MixedSetupData = setup(mixed),
    realtime_mixed_version_test(MixedSetupData),
    fullsync_mixed_version_test(MixedSetupData),
    cleanup(MixedSetupData, false),
    pass.

realtime_test({ClusterNodes, BucketTypes, PBA, PBB}) ->
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
    UndefObjTyped2 = riakc_obj:new(UndefBucketTyped, UndefKeyTyped, <<"data1">>),

    lager:info("doing typed put on A where type is not defined on B, bucket:~p", [UndefBucketTyped]),
    riakc_pb_socket:put(PBA, UndefObjTyped, [{w,3}]),
    riakc_pb_socket:put(PBA, UndefObjTyped2, [{w,3}]),

    lager:info("waiting for undefined type pb get on B, should get error <<\"no_type\">>"),

    ErrorResult = riakc_pb_socket:get(PBB, UndefBucketTyped, UndefKeyTyped),
    ?assertEqual({error, <<"no_type">>}, ErrorResult),

    % checking the rtq had drained on the source cluster
    ensure_rtq_drained(ANodes),

    DefaultProps = get_current_bucket_props(BNodes, DefinedType),
    ?assertEqual({n_val, 3}, lists:keyfind(n_val, 1, DefaultProps)),

    update_props(DefinedType, [{n_val, 1}], LeaderB, BNodes),
    ok = rt:wait_until(fun() ->
            UpdatedProps = get_current_bucket_props(BNodes, DefinedType),
            {n_val, 1} =:= lists:keyfind(n_val, 1, UpdatedProps)
        end),

    UnequalObjBin = <<"unequal props val">>,
    UnequalPropsObj = riakc_obj:new(BucketTyped, KeyTyped, UnequalObjBin),
    lager:info("doing put of typed bucket on A where bucket properties "
               "(n_val 3 versus n_val 1) are not equal on B"),
    riakc_pb_socket:put(PBA, UnequalPropsObj, [{w,3}]),

    lager:info("checking to ensure the bucket contents were not updated."),
    ensure_bucket_not_updated(PBB, BucketTyped, KeyTyped, Bin),
    disable_rt(LeaderA, ANodes),

    update_props(DefinedType, [{n_val, 3}], LeaderB, BNodes),
    ok = rt:wait_until(fun() ->
            UpdatedProps2 = get_current_bucket_props(BNodes, DefinedType),
            {n_val, 3} =:= lists:keyfind(n_val, 1, UpdatedProps2)
        end),

    disable_rt(LeaderA, ANodes).

realtime_mixed_version_test({ClusterNodes, BucketTypes, PBA, PBB}) ->
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

fullsync_test({ClusterNodes, BucketTypes, PBA, PBB}) ->
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

    update_props(DefinedType,  [{n_val, 1}], LeaderB, BNodes),
    ok = rt:wait_until(fun() ->
            UpdatedProps = get_current_bucket_props(BNodes, DefinedType),
            {n_val, 1} =:= lists:keyfind(n_val, 1, UpdatedProps)
        end),

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

fullsync_mixed_version_test({ClusterNodes, BucketTypes, PBA, PBB}) ->
    {LeaderA, LeaderB, ANodes, _BNodes} = ClusterNodes,
    {DefinedType, _UndefType} = BucketTypes,

    %% Enable RT replication from cluster "A" to cluster "B"
    lager:info("Enabling fullsync between ~p and ~p", [LeaderA, LeaderB]),
    enable_fullsync(LeaderA, ANodes),

    Bin = <<"good data">>,
    Key = <<"key">>,
    Bucket = <<"fullsync-kicked">>,
    DefaultObj = riakc_obj:new(Bucket, Key, Bin),
    lager:info("doing untyped put on A, bucket:~p", [Bucket]),
    riakc_pb_socket:put(PBA, DefaultObj, [{w,3}]),

    BucketTyped = {DefinedType, Bucket},
    KeyTyped = <<"keytyped">>,
    BadBin = <<"overwritten">>,
    ObjTyped = riakc_obj:new(BucketTyped, KeyTyped, BadBin),

    lager:info("doing typed put on A, bucket:~p", [BucketTyped]),
    riakc_pb_socket:put(PBA, ObjTyped, [{w,3}]),

    {SyncTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    lager:info("Fullsync completed in ~p seconds", [SyncTime1/1000/1000]),

    ReadResult1 = riakc_pb_socket:get(PBB, Bucket, Key),
    ?assertMatch({ok, _}, ReadResult1),

    %% The following check appears to be the best we can do. If a 2.x source
    %% sends a typed bucket to the 1.x sink, the put will occur.
    %% The bucket is undefined to the interfaces, but some parts of it
    %% appear to be written to the sink node. Since we cannot check using pb,
    %% here we at least make sure we haven't written over an existing default
    %% bucket with data from a typed bucket of the same name.
    ensure_bucket_not_updated(PBB, Bucket, Key, Bin).

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

%% @doc Connect two clusters using a given name.
connect_cluster(Source, Port, Name) ->
    lager:info("Connecting ~p to ~p for cluster ~p.",
               [Source, Port, Name]),
    repl_util:connect_cluster(Source, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Source, Name)).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    Port = repl_util:get_port(LeaderB),
    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

cluster_conf() ->
    [
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
    ].

deploy_nodes(NumNodes, current) ->
    rt_cluster:deploy_nodes(NumNodes, cluster_conf());
deploy_nodes(_, mixed) ->
    Conf = cluster_conf(),
    rt_cluster:deploy_nodes([{current, Conf}, {previous, Conf}]).

%% @doc Create two clusters of 1 node each and connect them for replication:
%%      Cluster "A" -> cluster "B"
make_clusters(Type) ->
    NumNodes = rt_config:get(num_nodes, 2),
    ClusterASize = rt_config:get(cluster_a_size, 1),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Nodes = deploy_nodes(NumNodes, Type),
    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    %% Name the clusters
    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Waiting for transfers to complete."),
    rt:wait_until_transfers_complete(ANodes),
    rt:wait_until_transfers_complete(BNodes),

    %% get the leader for the first cluster
    lager:info("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),

    %% get the leader for the second cluster
    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

    ALeader = repl_util:get_leader(hd(ANodes)),
    BLeader = repl_util:get_leader(hd(BNodes)),

    lager:info("ALeader: ~p BLeader: ~p", [ALeader, BLeader]),
    {ALeader, BLeader, ANodes, BNodes}.

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

ensure_rtq_drained(ANodes) ->
    lager:info("making sure the rtq has drained"),
    Got = lists:map(fun(Node) ->
                    [] =:= rpc:call(Node, riak_repl2_rtq, dumpq, [])
                    end, ANodes),
    Expected = [true || _ <- lists:seq(1, length(ANodes))],
    ?assertEqual(Expected, Got).
