-module(ts_replication).
-behavior(riak_test).
-export([confirm/0, replication/2]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [join/2,
             log_to_nodes/2,
             log_to_nodes/3,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

confirm() ->
    rt:set_backend(eleveldb),
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

    %% TS-ize the clusters
    DDL = ts_util:get_ddl(),
    Bucket = ts_util:get_default_bucket(),
    ts_util:create_table(normal, ANodes, DDL, Bucket),
    ts_util:create_table(normal, BNodes, DDL, Bucket),

    replication(ANodes, BNodes),
    pass.

qty_records_present(Node, Lower, Upper) ->
    %% Queries use strictly greater/less than
    Qry = ts_util:get_valid_qry(Lower-1, Upper+1),
    {_Hdrs, Results} = riakc_ts:query(rt:pbc(Node), Qry),
    length(Results).

put_records(Node, Lower, Upper) ->
    riakc_ts:put(rt:pbc(Node), ts_util:get_default_bucket(),
                 ts_util:get_valid_select_data(
                   fun() -> lists:seq(Lower, Upper) end)).

replication(ANodes, BNodes) ->

    log_to_nodes(ANodes ++ BNodes, "Starting ts_replication test"),

    lager:info("Real Time Replication test"),
    real_time_replication_test(ANodes, BNodes),

    lager:info("Tests passed"),

    fin.



%% @doc Real time replication test
real_time_replication_test([AFirst|_] = ANodes, [BFirst|_] = BNodes) ->

    %% Before connecting clusters, write some initial data to Cluster A
    lager:info("Writing 100 keys to ~p", [AFirst]),
    ?assertEqual(ok, put_records(AFirst, 1, 100)),
    ?assertEqual(100, qty_records_present(AFirst, 1, 100)),

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

    lager:info("Verifying none of the records on A are on B"),
    ?assertEqual(0, qty_records_present(BFirst, 1, 100)),

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify replication to Cluster B via realtime"),
    lager:info("Writing 100 keys to Cluster A-LeaderNode: ~p", [LeaderA]),
    ?assertEqual(ok, put_records(AFirst, 101, 200)),

    lager:info("Reading 100 keys written to Cluster A-LeaderNode: ~p from Cluster B-Node: ~p", [LeaderA, BFirst]),
    ok = rt:wait_until(fun() -> 100 == qty_records_present(BFirst, 101, 200) end).
