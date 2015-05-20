%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% -------------------------------------------------------------------
-module(repl_aae_fullsync_bt).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(ENSURE_READ_ITERATIONS, 5).
-define(ENSURE_READ_INTERVAL, 1000).

%% Replication Bucket Types test
%%

setup(Type) ->
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    {LeaderA, LeaderB, ANodes, BNodes} = ClusterNodes = make_clusters(Type),

    PBA = rt:pbc(LeaderA),
    PBB = rt:pbc(LeaderB),

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
            ok;
        aae ->
            rt:create_and_activate_bucket_type(LeaderB,
                                               DefinedType,
                                               [{n_val, 3}, {allow_mult, false}]),
            rt:wait_until_bucket_type_status(DefinedType, active, BNodes),
            rt:wait_until_bucket_type_visible(BNodes, DefinedType)
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
            rt:clean_cluster(ANodes ++ BNodes);
        false ->
            ok
    end.

%% @doc riak_test entry point
confirm() ->
    AAESetupData = setup(aae),
    aae_fullsync_test(AAESetupData),
    cleanup(AAESetupData, true),
    pass.

aae_fullsync_test({ClusterNodes, BucketTypes, PBA, PBB}) ->
    {LeaderA, LeaderB, ANodes, BNodes} = ClusterNodes,
    {DefinedType, UndefType} = BucketTypes,

    lager:info("Enabling AAE fullsync between ~p and ~p", [LeaderA, LeaderB]),
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

    lager:info("waiting for AAE trees to build on all nodes"),
    rt:wait_until_aae_trees_built(ANodes),
    rt:wait_until_aae_trees_built(BNodes),

    perform_sacrifice(LeaderA),

    {SyncTime1, _} = timer:tc(repl_util,
                              start_and_wait_until_fullsync_complete,
                              [LeaderA]),

    lager:info("AAE Fullsync completed in ~p seconds", [SyncTime1/1000/1000]),

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

    lager:info("AAE Fullsync completed in ~p seconds", [SyncTime2/1000/1000]),

    lager:info("checking to ensure the bucket contents were not updated."),
    ensure_bucket_not_updated(PBB, BucketTyped, KeyTyped, Bin).

%% @doc Turn on fullsync replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_fullsync(LeaderA, ANodes) ->
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    Port = repl_util:get_port(LeaderB),
    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

cluster_conf_aae() ->
    [
     {riak_core,
            [
             {ring_creation_size, 8}
            ]
        },
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
          {fullsync_strategy, aae},
          {fullsync_on_connect, false},
          {fullsync_interval, disabled},
          {max_fssource_soft_retries, 10},
          {max_fssource_retries, infinity}
         ]}
        ].

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
    rt:deploy_nodes(NumNodes, cluster_conf(), [riak_kv, riak_repl]);
deploy_nodes(NumNodes, aae) ->
    rt:deploy_nodes(NumNodes, cluster_conf_aae(), [riak_kv, riak_repl]);
deploy_nodes(_, mixed) ->
    Conf = cluster_conf(),
    rt:deploy_nodes([{current, Conf}, {previous, Conf}], [riak_kv, riak_repl]).

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

%% @doc Required for 1.4+ Riak, write sacrificial keys to force AAE
%%      trees to flush to disk.
perform_sacrifice(Node) ->
    ?assertEqual([], repl_util:do_write(Node, 1, 2000,
                                        <<"sacrificial">>, 1)).
