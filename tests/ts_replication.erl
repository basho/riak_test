-module(ts_replication).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, rt_intercept_pt}).

%% Test both realtime and fullsync replication for timeseries data.

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
                {ts_realtime, true},
                {diff_batch_size, 10}
             ]}
    ],

    lager:info("Building Clusters A and B"),
    [ANodes, BNodes] = rt:build_clusters([{ClusterASize, Conf}, {NumNodes - ClusterASize, Conf}]),

    %% TS-ize the clusters
    DDL = ts_util:get_ddl(),
    Table = ts_util:get_default_bucket(),
    ts_util:create_table(normal, ANodes, DDL, Table),
    ts_util:create_table(normal, BNodes, DDL, Table),

    replication(ANodes, BNodes, list_to_binary(Table), <<"hey look ma no w1c">>),
    test_ddl_comparison(ANodes, BNodes),
    pass.

test_ddl_comparison(ANodes, BNodes) ->
    lager:info("Testing TS realtime fails with incompatible DDLs"),
    Table = "house_of_horrors",
    SmallDDL = ts_util:get_ddl(small, Table),
    BigDDL = ts_util:get_ddl(big, Table),
    ts_util:create_table(normal, ANodes, SmallDDL, Table),
    ts_util:create_table(normal, BNodes, BigDDL, Table),
    LeaderA = get_leader(hd(ANodes)),
    PortB = get_mgr_port(hd(BNodes)),
    prop_failure_replication_test(ANodes, BNodes, LeaderA, PortB, Table).

create_normal_type(Nodes, BucketType) ->
    TypeProps = [{n_val, 3}],
    lager:info("Create bucket type ~p, wait for propagation", [BucketType]),
    rt:create_and_activate_bucket_type(hd(Nodes), BucketType, TypeProps),
    rt:wait_until_bucket_type_status(BucketType, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {BucketType, <<"bucket">>}, TypeProps).

ts_num_records_present(Node, Lower, Upper, Table) when is_binary(Table) ->
    ts_num_records_present(Node, Lower, Upper, unicode:characters_to_list(Table));
ts_num_records_present(Node, Lower, Upper, Table) ->
    %% Queries use strictly greater/less than
    Qry = ts_util:get_valid_qry(Lower-1, Upper+1, Table),
    {ok, {_Hdrs, Results}} = riakc_ts:query(rt:pbc(Node), Qry),
    length(Results).

kv_num_objects_present(Node, Lower, Upper, Bucket) ->
    FailedMatches = rt:systest_read(Node, Lower, Upper, Bucket, 2),
    PotentialQty = Upper - Lower + 1,
    PotentialQty - length(FailedMatches).

delete_record(Node, Table, Time) ->
    [RecordAsTuple] = ts_util:get_valid_select_data(fun() -> lists:seq(Time, Time) end),
    RecordAsList = tuple_to_list(RecordAsTuple),
    KeyAsList = lists:sublist(RecordAsList, 3),
    lager:info("Deleting ~p from ~ts~n", [KeyAsList, Table]),
    riakc_ts:delete(rt:pbc(Node), Table, KeyAsList, []).

put_records(Node, Table, Lower, Upper) ->
    riakc_ts:put(rt:pbc(Node), Table,
                 ts_util:get_valid_select_data(
                   fun() -> lists:seq(Lower, Upper) end)).

replication(ANodes, BNodes, Table, NormalType) ->

    log_to_nodes(ANodes ++ BNodes, "Starting ts_replication test"),

    LeaderA = get_leader(hd(ANodes)),
    PortB = get_mgr_port(hd(BNodes)),

    KVBucket = {NormalType, <<"bucket">>},
    KVBucketInTS = {Table, <<"bucket">>},

    create_normal_type(ANodes, NormalType),
    create_normal_type(BNodes, NormalType),

    %% Make certain we don't start throwing objects into the realtime
    %% queue until realtime sync is enabled. Bug in 1.3, fixed in 1.4.
    lager:info("Adding intercept to RTQ"),
    rt_intercept:add(hd(ANodes),
                     {riak_repl2_rtq,
                      [{
                         {push, 3},
                         {[],
                          fun(_Len, _Bin, _Meta) ->
                                  application:set_env(riak_repl, '_test_rtq_pushed', true)
                          end
                         }
                       }
                      ]
                     }),

    %% Before connecting clusters, write some initial data to Cluster A
    lager:info("Writing 100 KV values to ~p (~p)", [hd(ANodes), KVBucket]),
    ?assertEqual([], repl_util:do_write(hd(ANodes), 1, 100, KVBucket, 2)),

    lager:info("Writing 100 KV values to ~p (~p)", [hd(ANodes), KVBucketInTS]),
    ?assertEqual([], repl_util:do_write(hd(ANodes), 1, 100, KVBucketInTS, 2)),

    lager:info("Writing 100 TS records to ~p (~p)", [hd(ANodes), Table]),
    ?assertEqual(ok, put_records(hd(ANodes), Table, 1, 100)),
    ?assertEqual(100, ts_num_records_present(hd(ANodes), 1, 100, Table)),

    %% Now remove our temporary intercept
    rt_intercept:clean(hd(ANodes), riak_repl2_rtq),

    %% Check to see if anything was pushed to the realtime queue
    lager:info("Checking to see whether anything was (wrongly) pushed to the RTQ"),
    WasPushed = rpc:call(hd(ANodes), application, get_env, [riak_repl, '_test_rtq_pushed']),
    ?assertEqual(undefined, WasPushed),

    lager:info("Testing a non-w1c bucket type (realtime)"),
    real_time_replication_test(ANodes, BNodes, LeaderA, PortB, KVBucket),

    %% We do not have realtime sync for w1c, but make sure we can
    %% still write and read the data from the source cluster
    lager:info("Testing the timeseries bucket type, non-ts-managed bucket (realtime)"),
    no_w1c_real_time_replication_test(ANodes, BNodes, LeaderA, PortB, KVBucketInTS),

    lager:info("Testing timeseries data (realtime)"),
    ts_real_time_replication_test(ANodes, BNodes, LeaderA, PortB, Table),

    lager:info("Testing a change of bucket properties to disable realtime"),
    no_ts_real_time_replication_test(ANodes, BNodes, LeaderA, PortB, Table),

    lager:info("Testing all buckets with fullsync"),
    full_sync_replication_test(ANodes, BNodes, LeaderA, PortB, KVBucket, KVBucketInTS, Table),

    lager:info("Tests passed"),

    fin.

full_sync_replication_test([AFirst|_]=ANodes, [BFirst|_]=BNodes, LeaderA, PortB, KVBucket, KVBucketInTS, Table) ->
    BNode = hd(BNodes),

    %% Revisit data written before realtime was tested to verify that
    %% it is still absent on the sink cluster
    lager:info("Verifying first 100 keys not present on 2nd cluster (non-w1c)"),
    ?assertEqual(0, kv_num_objects_present(BNode, 1, 100, KVBucket)),
    lager:info("Verifying first 100 keys not present on 2nd cluster (ts bucket type, non-ts bucket)"),
    ?assertEqual(0, kv_num_objects_present(BNode, 1, 100, KVBucketInTS)),
    lager:info("Verifying first 100 TS keys not present on 2nd cluster"),
    ?assertEqual(0, ts_num_records_present(BNode, 1, 100, Table)),

    lager:info("Starting and waiting for fullsync"),
    connect_clusters(ANodes, BNodes, LeaderA, PortB),
    start_mdc(ANodes, LeaderA, "B", true),
    repl_util:start_and_wait_until_fullsync_complete(hd(ANodes), "B"),

    lager:info("Verifying first 100 keys present on 2nd cluster (non-w1c)"),
    ?assertEqual(100, kv_num_objects_present(BNode, 1, 100, KVBucket)),
    lager:info("Verifying first 100 keys present on 2nd cluster (ts bucket type, non-ts bucket)"),
    ?assertEqual(100, kv_num_objects_present(BNode, 1, 100, KVBucketInTS)),
    lager:info("Verifying first 100 TS keys present on 2nd cluster"),
    ?assertEqual(100, ts_num_records_present(BNode, 1, 100, Table)),

    lager:info("Deleting a record on Cluster A"),
    delete_record(AFirst, Table, 23),
    timer:sleep(500),
    lager:info("Verifying record is no longer on Cluster A"),
    ?assertEqual(0, ts_num_records_present(AFirst, 23, 23, Table)),
    repl_util:start_and_wait_until_fullsync_complete(hd(ANodes), "B"),
    lager:info("Verifying record is no longer on Cluster B"),
    ?assertEqual(0, ts_num_records_present(BFirst, 23, 23, Table)),

    disconnect_clusters(ANodes, LeaderA, "B").

real_time_replication_test(ANodes, [BFirst|_] = BNodes, LeaderA, PortB, Bucket) ->
    lager:info("Connecting clusters"),
    connect_clusters(ANodes, BNodes, LeaderA, PortB),
    lager:info("Starting real-time MDC"),
    start_mdc(ANodes, LeaderA, "B", false),

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify replication to Cluster B via realtime"),
    lager:info("Writing 100 keys to Cluster A-LeaderNode: ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 101, 200, Bucket, 2)),

    lager:info("Reading 100 keys written to Cluster A-LeaderNode: ~p from Cluster B-Node: ~p", [LeaderA, PortB]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 200, Bucket, 2)),

    disconnect_clusters(ANodes, LeaderA, "B").


%% @doc Real time replication test
ts_real_time_replication_test([AFirst|_] = ANodes, [BFirst|_] = BNodes, LeaderA, PortB, Table) ->
    connect_clusters(ANodes, BNodes, LeaderA, PortB),
    start_mdc(ANodes, LeaderA, "B", false),

    lager:info("Verifying none of the records on A are on B"),
    ?assertEqual(0, ts_num_records_present(BFirst, 1, 100, Table)),

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify replication to Cluster B via realtime"),
    lager:info("Writing 100 keys to Cluster A-LeaderNode: ~p", [LeaderA]),
    ?assertEqual(ok, put_records(AFirst, Table, 101, 200)),

    lager:info("Writing a single key to Cluster A-LeaderNode: ~p", [LeaderA]),
    ?assertEqual(ok, put_records(AFirst, Table, 201, 201)),

    lager:info("Reading 101 keys written to Cluster A-LeaderNode: ~p from Cluster B-Node: ~p", [LeaderA, BFirst]),
    ?assertEqual(ok, rt:wait_until(fun() -> 101 == ts_num_records_present(BFirst, 101, 201, Table) end)),

    lager:info("Deleting a record on Cluster A"),
    delete_record(AFirst, Table, 174),
    timer:sleep(500),
    lager:info("Verifying record is no longer on Cluster A"),
    ?assertEqual(0, ts_num_records_present(AFirst, 174, 174, Table)),
    lager:info("Verifying record is no longer on Cluster B"),
    ?assertEqual(0, ts_num_records_present(BFirst, 174, 174, Table)),


    disconnect_clusters(ANodes, LeaderA, "B").


%% @doc No w1c replication test (feature is not yet implemented)
no_w1c_real_time_replication_test([AFirst|_] = ANodes, [BFirst|_] = BNodes, LeaderA, PortB, Bucket) ->
    connect_clusters(ANodes, BNodes, LeaderA, PortB),
    start_mdc(ANodes, LeaderA, "B", false),

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify no replication to Cluster B via realtime"),
    lager:info("Writing 100 keys to Cluster A-LeaderNode: ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 1001, 1100, Bucket, 2)),

    lager:info("Verifying all of the new records are on A"),
    ?assertEqual(0, repl_util:wait_for_reads(AFirst, 1001, 1100, Bucket, 2)),

    lager:info("Pausing 2 seconds"),
    timer:sleep(2000),

    lager:info("Verifying none of the new records are on B"),
    ?assertEqual(true, repl_util:confirm_missing(BFirst, 1001, 1100, Bucket, 2)),

    disconnect_clusters(ANodes, LeaderA, "B").

%% @doc No real time replication test (bucket properties set to fullsync)
no_ts_real_time_replication_test([AFirst|_] = ANodes, [BFirst|_] = BNodes, LeaderA, PortB, Table) ->
    connect_clusters(ANodes, BNodes, LeaderA, PortB),
    start_mdc(ANodes, LeaderA, "B", false),

    %% Set bucket properties to {repl, fullsync} before writing data
    rt:pbc_set_bucket_type(rt:pbc(AFirst), Table, [{repl, fullsync}]),

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify no replication to Cluster B via realtime"),
    lager:info("Writing 100 keys to Cluster A-LeaderNode: ~p", [LeaderA]),
    ?assertEqual(ok, put_records(AFirst, Table, 202, 301)),

    lager:info("Pausing 2 seconds"),
    timer:sleep(2000),

    lager:info("Verifying none of the new records are on B"),
    ?assertEqual(0, ts_num_records_present(BFirst, 202, 301, Table)),

    %% "Undo" (sort of) the bucket property
    rt:pbc_set_bucket_type(rt:pbc(AFirst), Table, [{repl, both}]),

    disconnect_clusters(ANodes, LeaderA, "B").

%% @doc No real time replication test (incompatible DDLs)
prop_failure_replication_test([AFirst|_] = ANodes, [BFirst|_] = BNodes, LeaderA, PortB, Table) ->
    connect_clusters(ANodes, BNodes, LeaderA, PortB),
    start_mdc(ANodes, LeaderA, "B", false),

    log_to_nodes(ANodes++BNodes, "Write data to Cluster A, verify no replication to Cluster B via realtime because DDL comparison failed"),
    lager:info("Writing 100 keys to Cluster A-LeaderNode: ~p", [LeaderA]),
    ?assertEqual(ok, put_records(AFirst, Table, 202, 301)),

    lager:info("Pausing 2 seconds"),
    timer:sleep(2000),

    lager:info("Verifying none of the new records are on B"),
    ?assertEqual(0, ts_num_records_present(BFirst, 202, 301, Table)),

    disconnect_clusters(ANodes, LeaderA, "B").

connect_clusters([AFirst|_] = ANodes, [BFirst|_] = BNodes,
                 LeaderA, PortB) ->
    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),

    %% Wait for Cluster naming to converge.
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    lager:info("Waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),

    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, PortB]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", PortB),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

disconnect_clusters(SourceNodes, SourceLeader, RemoteClusterName) ->
    lager:info("Disconnect the 2 clusters"),
    repl_util:disable_realtime(SourceLeader, RemoteClusterName),
    rt:wait_until_ring_converged(SourceNodes),
    repl_util:disconnect_cluster(SourceLeader, RemoteClusterName),
    repl_util:wait_until_no_connection(SourceLeader),
    rt:wait_until_ring_converged(SourceNodes).

%% Last argument: do fullsync or not
start_mdc(SourceNodes, SourceLeader, RemoteClusterName, true) ->
    start_mdc(SourceNodes, SourceLeader, RemoteClusterName, false),
    repl_util:enable_fullsync(SourceLeader, RemoteClusterName),
    rt:wait_until_ring_converged(SourceNodes);
start_mdc(SourceNodes, SourceLeader, RemoteClusterName, false) ->
    repl_util:enable_realtime(SourceLeader, RemoteClusterName),
    rt:wait_until_ring_converged(SourceNodes),
    repl_util:start_realtime(SourceLeader, RemoteClusterName),
    rt:wait_until_ring_converged(SourceNodes).

get_leader(Node) ->
    rpc:call(Node, riak_core_cluster_mgr, get_leader, []).

get_mgr_port(Node) ->
    {ok, {_IP, Port}} = rpc:call(Node, application, get_env, [riak_core, cluster_mgr]),
    Port.
