-module(repl_util).
-export([make_cluster/1,
         name_cluster/2,
         node_has_version/2,
         nodes_with_version/2,
         nodes_all_have_version/2,
         wait_until_is_leader/1,
         is_leader/1,
         wait_until_is_not_leader/1,
         wait_until_leader/1,
         wait_until_new_leader/2,
         wait_until_leader_converge/1,
         wait_until_connection/1,
         wait_until_no_connection/1,
         wait_for_reads/5,
         wait_until_fullsync_started/1,
         wait_until_fullsync_stopped/1,
         start_and_wait_until_fullsync_complete/1,
         start_and_wait_until_fullsync_complete/2,
         start_and_wait_until_fullsync_complete/3,
         start_and_wait_until_fullsync_complete/4,
         connect_cluster/3,
         disconnect_cluster/2,
         wait_for_connection/2,
         wait_for_disconnect/2,
         wait_for_full_disconnect/1,
         wait_until_connection_errors/2,
         wait_until_connections_clear/1,
         enable_realtime/2,
         disable_realtime/2,
         enable_fullsync/2,
         start_realtime/2,
         stop_realtime/2,
         stop_fullsync/2,
         disable_fullsync/2,
         do_write/5,
         get_fs_coord_status_item/3,
         num_partitions/1,
         get_cluster_mgr_port/1,
         maybe_reconnect_rt/3,
         connect_rt/3,
         connect_cluster_by_name/3,
         connect_cluster_by_name/4,
         get_port/1,
         get_leader/1,
         verify_connectivity/1,
         write_to_cluster/4,
         write_to_cluster/5,
         read_from_cluster/5,
         read_from_cluster/6,
         check_fullsync/3,
         validate_completed_fullsync/6,
         validate_intercepted_fullsync/5,
         get_aae_fullsync_activity/0,
         activate_debug_for_validate_aae_fullsync/1,
         validate_aae_fullsync/6,
         update_props/5,
         get_current_bucket_props/2,
         create_clusters_with_rt/2,
         deploy_clusters_with_rt/2,
         setup_rt/3,
         verify_correct_connection/1,
         verify_correct_connection/2
        ]).
-include_lib("eunit/include/eunit.hrl").

make_cluster(Nodes) ->
    [First|Rest] = Nodes,
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    [rt:wait_for_service(N, riak_kv) || N <- Nodes],
    [rt:join(Node, First) || Node <- Rest],
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)).

name_cluster(Node, Name) ->
    lager:info("Naming cluster ~p",[Name]),
    Res = rpc:call(Node, riak_repl_console, clustername, [[Name]]),
    ?assertEqual(ok, Res).

wait_until_is_leader(Node) ->
    lager:info("wait_until_is_leader(~p)", [Node]),
    rt:wait_until(Node, fun is_leader/1).

is_leader(Node) ->
    case rpc:call(Node, riak_core_cluster_mgr, get_leader, []) of
        {badrpc, Wut} ->
            lager:info("Badrpc during is_leader for ~p. Error: ~p", [Node, Wut]),
            false;
        Leader ->
            lager:info("Checking: ~p =:= ~p", [Leader, Node]),
            Leader =:= Node
    end.


wait_until_is_not_leader(Node) ->
    lager:info("wait_until_is_not_leader(~p)", [Node]),
    rt:wait_until(Node, fun is_not_leader/1).

is_not_leader(Node) ->
    case rpc:call(Node, riak_core_cluster_mgr, get_leader, []) of
        {badrpc, Wut} ->
            lager:info("Badrpc during is_not leader for ~p. Error: ~p", [Node, Wut]),
            false;
        Leader ->
            lager:info("Checking: ~p =/= ~p", [Leader, Node]),
            Leader =/= Node
    end.

wait_until_leader(Node) ->
    wait_until_new_leader(Node, undefined).

wait_until_new_leader(Node, OldLeader) ->
    Res = rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_core_cluster_mgr, get_leader, []),
                case Status of
                    {badrpc, _} ->
                        false;
                    undefined ->
                        false;
                    OldLeader ->
                        false;
                    _Other ->
                        true
                end
        end),
    ?assertEqual(ok, Res).

wait_until_leader_converge([Node|_] = Nodes) ->
    rt:wait_until(Node,
        fun(_) ->
                LeaderResults =
                    [rpc:call(N, riak_core_cluster_mgr, get_leader, []) ||
                        N <- Nodes],
                {Leaders, Errors} =
                    lists:partition(leader_result_filter_fun(), LeaderResults),
                UniqueLeaders = lists:usort(Leaders),
                Errors == [] andalso length(UniqueLeaders) == 1
        end).

leader_result_filter_fun() ->
    fun(L) ->
            case L of
                undefined ->
                    false;
                {badrpc, _} ->
                    false;
                _ ->
                    true
            end
    end.

wait_until_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case Status of
                    {badrpc, _} ->
                        false;
                    _ ->
                        case proplists:get_value(fullsync_coordinator, Status) of
                            [] ->
                                false;
                            [_C] ->
                                true;
                            Conns ->
                                lager:warning("multiple connections detected: ~p",
                                              [Conns]),
                                true
                        end
                end
        end). %% 40 seconds is enough for repl

wait_until_no_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case Status of
                    {badrpc, _} ->
                        false;
                    _ ->
                        case proplists:get_value(connected_clusters, Status) of
                            [] ->
                                true;
                            _ ->
                                false
                        end
                end
        end). %% 40 seconds is enough for repl

wait_until_fullsync_started(SourceLeader) ->
    rt:wait_until(fun() ->
                     lager:info("Waiting for fullsync to start"),
                     Coordinators = [Pid || {"B", Pid} <-
                         riak_repl2_fscoordinator_sup:started(SourceLeader)],
                     lists:any(fun riak_repl2_fscoordinator:is_running/1,
                         Coordinators)
                  end).

wait_until_fullsync_stopped(SourceLeader) ->
    rt:wait_until(fun() ->
                     lager:info("Waiting for fullsync to stop"),
                     Coordinators = [Pid || {"B", Pid} <-
                         riak_repl2_fscoordinator_sup:started(SourceLeader)],
                     not lists:any(fun riak_repl2_fscoordinator:is_running/1,
                         Coordinators)
                  end).

wait_for_reads(Node, Start, End, Bucket, R) ->
    rt:wait_until(Node,
        fun(_) ->
                Reads = rt:systest_read(Node, Start, End, Bucket, R, <<>>, true),
                Reads == []
        end),
    Reads = rt:systest_read(Node, Start, End, Bucket, R, <<>>, true),
    lager:info("Reads: ~p", [Reads]),
    length(Reads).

get_fs_coord_status_item(Node, SinkName, ItemName) ->
    Status = rpc:call(Node, riak_repl_console, status, [quiet]),
    FS_CoordProps = proplists:get_value(fullsync_coordinator, Status),
    ClusterProps = proplists:get_value(SinkName, FS_CoordProps),
    proplists:get_value(ItemName, ClusterProps).

start_and_wait_until_fullsync_complete(Node) ->
    start_and_wait_until_fullsync_complete(Node, undefined).

start_and_wait_until_fullsync_complete(Node, Cluster) ->
    start_and_wait_until_fullsync_complete(Node, Cluster, undefined).

start_and_wait_until_fullsync_complete(Node, Cluster, NotifyPid) ->
    start_and_wait_until_fullsync_complete(Node, Cluster, NotifyPid, 20).

start_and_wait_until_fullsync_complete(Node, Cluster, NotifyPid, Retries) ->
    Status0 = rpc:call(Node, riak_repl_console, status, [quiet]),
    Count0 = proplists:get_value(server_fullsyncs, Status0),
    Count = fullsync_count(Count0, Status0, Cluster),

    lager:info("Waiting for fullsync count to be ~p", [Count]),

    lager:info("Starting fullsync on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, fullsync, [fullsync_start_args(Cluster)]),

    %% sleep because of the old bug where stats will crash if you call it too
    %% soon after starting a fullsync
    timer:sleep(500),

    %% Send message to process and notify fullsync has began.
    fullsync_notify(NotifyPid),

    %% case rt:wait_until(make_fullsync_wait_fun(Node, Count), 100, 1000) of
    case rt:wait_until(make_fullsync_wait_fun(Node, Count)) of
        ok ->
            ok;
        _  when Retries > 0 ->
            ?assertEqual(ok, wait_until_connection(Node)),
            lager:warning("Node failed to fullsync, retrying"),
            start_and_wait_until_fullsync_complete(Node, Cluster, NotifyPid, Retries-1)
    end,
    lager:info("Fullsync on ~p complete", [Node]).

fullsync_count(Count, Status, undefined) ->
    %% count the # of fullsync enabled clusters
    FullSyncClusters = proplists:get_value(fullsync_enabled, Status),
    Count + length(string:tokens(FullSyncClusters, ", "));
fullsync_count(Count, _Status, _Cluster) ->
      Count + 1.

fullsync_start_args(undefined) ->
    lager:info("No cluster for fullsync start"),
    ["start"];
fullsync_start_args(Cluster) ->
    lager:info("Cluster for fullsync start: ~p", [Cluster]),
    ["start", Cluster].

fullsync_notify(NotifyPid) when is_pid(NotifyPid) ->
            NotifyPid ! fullsync_started;
fullsync_notify(_) ->
    ok.

make_fullsync_wait_fun(Node, Count) ->
    fun() ->
            Status = rpc:call(Node, riak_repl_console, status, [quiet]),
            case Status of
                {badrpc, _} ->
                    false;
                _ ->
                    case proplists:get_value(server_fullsyncs, Status) of
                        C when C >= Count ->
                            true;
                        _ ->
                            false
                    end
            end
    end.

connect_cluster(Node, IP, Port) ->
    Res = rpc:call(Node, riak_repl_console, connect,
        [[IP, integer_to_list(Port)]]),
    ?assertEqual(ok, Res).

disconnect_cluster(Node, Name) ->
    Res = rpc:call(Node, riak_repl_console, disconnect,
        [[Name]]),
    ?assertEqual(ok, Res).

wait_for_connection(Node, Name) ->
    rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_core_cluster_mgr,
                        get_connections, []) of
                    {ok, Connections} ->
                        Conn = [P || {{cluster_by_name, N}, P} <- Connections, N == Name],
                        case Conn of
                            [] ->
                                false;
                            [Pid] ->
                                try riak_core_cluster_conn:status(Pid, 2) of                                    {Pid, status, _} ->
                                        true;
                                    _ ->
                                        false
                                catch
                                    _W:_Y ->
                                        Pid ! {self(), status},
                                        receive
                                            {Pid, status, _} ->
                                                true;
                                            {Pid, connecting, _} ->
                                                false
                                        end
                                end
                        end;
                    _ ->
                        false
                end
        end).

%% @doc Wait for disconnect from this node to the
%%      named cluster.
wait_for_disconnect(Node, Name) ->
    rt:wait_until(Node, fun(_) ->
                lager:info("Attempting to verify disconnect on ~p from ~p.",
                           [Node, Name]),
                try
                    {ok, Connections} = rpc:call(Node,
                                                 riak_core_cluster_mgr,
                                                 get_connections,
                                                 []),
                    lager:info("Waiting for sink disconnect on ~p: ~p.",
                               [Node, Connections]),
                    Conn = [P || {{cluster_by_name, N}, P} <- Connections, N == Name],
                    case Conn of
                        [] ->
                            true;
                        _ ->
                            false
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).

%% @doc Wait for full disconnect from all clusters and IP's
wait_for_full_disconnect(Node) ->
    rt:wait_until(Node, fun(_) ->
                lager:info("Attempting to verify full disconnect on ~p.",
                           [Node]),
                try
                    {ok, Connections} = rpc:call(Node,
                                                 riak_core_cluster_mgr,
                                                 get_connections,
                                                 []),
                    lager:info("Waiting for sink disconnect on ~p: ~p.",
                               [Node, Connections]),
                    case Connections of
                        [] ->
                            true;
                        _ ->
                            false
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).

%% @doc Wait until canceled connections are cleared
wait_until_connections_clear(Node) ->
    rt:wait_until(Node, fun(_) ->
                try
                    Status = rpc:call(Node,
                                     riak_core_connection_mgr,
                                     get_request_states,
                                     []),
                    lager:info("Waiting for cancelled connections to clear on ~p: ~p.",
                               [Node, Status]),
                    case Status of
                        [] ->
                            true;
                        _ ->
                            false
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).

%% @doc Wait until errors in connection
wait_until_connection_errors(Node, BNode) ->
    {ok, {_IP, Port}} = rpc:call(BNode, application, get_env,
                                 [riak_core, cluster_mgr]),
    rt:wait_until(Node, fun(_) ->
                try
                    Failures = rpc:call(Node,
                                       riak_core_connection_mgr,
                                       get_connection_errors,
                                       [{"127.0.0.1",Port}]),
                    lager:info("Waiting for endpoint connection failures on ~p: ~p.",
                               [Node, Failures]),
                    case orddict:size(Failures) of
                        0 ->
                            false;
                        _ ->
                            true
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).

enable_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["enable", Cluster]]),
    ?assertEqual(ok, Res).

disable_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["disable", Cluster]]),
    ?assertEqual(ok, Res).

enable_fullsync(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, fullsync, [["enable", Cluster]]),
    ?assertEqual(ok, Res).

disable_fullsync(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, fullsync, [["disable", Cluster]]),
    ?assertEqual(ok, Res).

stop_fullsync(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, fullsync, [["stop", Cluster]]),
    ?assertEqual(ok, Res).

start_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["start", Cluster]]),
    ?assertEqual(ok, Res).

stop_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["stop", Cluster]]),
    ?assertEqual(ok, Res).

%% @doc Verify connectivity between sources and sink.
verify_connectivity(Node) ->
    rt:wait_until(Node, fun(N) ->
                {ok, Connections} = rpc:call(N,
                                             riak_core_cluster_mgr,
                                             get_connections,
                                             []),
                lager:info("Waiting for sink connections on ~p: ~p.",
                           [Node, Connections]),
                Connections =/= []
        end).

do_write(Node, Start, End, Bucket, W) ->
    case rt:systest_write(Node, Start, End, Bucket, W) of
        [] ->
            [];
        Errors ->
            lager:warning("~p errors while writing: ~p",
                [length(Errors), Errors]),
            timer:sleep(1000),
            lists:flatten([rt:systest_write(Node, S, S, Bucket, W) ||
                    {S, _Error} <- Errors])
    end.

%% does the node meet the version requirement?
node_has_version(Node, Version) ->
    NodeVersion =  rtdev:node_version(rtdev:node_id(Node)),
    case NodeVersion of
        current ->
            %% current always satisfies any version check
            true;
        _ ->
            NodeVersion >= Version
    end.

nodes_with_version(Nodes, Version) ->
    [Node || Node <- Nodes, node_has_version(Node, Version)].

nodes_all_have_version(Nodes, Version) ->
    Nodes == nodes_with_version(Nodes, Version).

%% Return the number of partitions in the cluster where Node is a member.
num_partitions(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    N = riak_core_ring:num_partitions(Ring),
    N.

get_cluster_mgr_port(Node) ->
    {ok, {_Ip, Port}} = rpc:call(Node, application, get_env, [riak_core, cluster_mgr]),
    Port.

maybe_reconnect_rt(SourceNode, SinkPort, SinkName) ->
    case repl_util:wait_for_connection(SourceNode, SinkName) of
        fail ->
            connect_rt(SourceNode, SinkPort, SinkName);
        Oot ->
            Oot
    end.

connect_rt(SourceNode, SinkPort, SinkName) ->
    repl_util:connect_cluster(SourceNode, "127.0.0.1", SinkPort),
    repl_util:wait_for_connection(SourceNode, SinkName),
    repl_util:enable_realtime(SourceNode, SinkName),
    repl_util:start_realtime(SourceNode, SinkName).

%% @doc Connect two clusters using a given name.
connect_cluster_by_name(Source, Port, Name) ->
    lager:info("Connecting ~p to ~p for cluster ~p.",
               [Source, Port, Name]),
    repl_util:connect_cluster(Source, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Source, Name)).

%% @doc Connect two clusters using a given name.
connect_cluster_by_name(Source, Destination, Port, Name) ->
    lager:info("Connecting ~p to ~p for cluster ~p.",
               [Source, Port, Name]),
    repl_util:connect_cluster(Source, Destination, Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Source, Name)).

%% @doc Given a node, find the port that the cluster manager is
%%      listening on.
get_port(Node) ->
    {ok, {_IP, Port}} = rpc:call(Node,
                                 application,
                                 get_env,
                                 [riak_core, cluster_mgr]),
    Port.

%% @doc Given a node, find out who the current replication leader in its
%%      cluster is.
get_leader(Node) ->
    rpc:call(Node, riak_core_cluster_mgr, get_leader, []).

%% @doc Validate fullsync completed and all keys are available.
validate_completed_fullsync(ReplicationLeader,
                            DestinationNode,
                            DestinationCluster,
                            Start,
                            End,
                            Bucket) ->
    ok = check_fullsync(ReplicationLeader, DestinationCluster, 0),
    lager:info("Verify: Reading ~p keys repl'd from A(~p) to ~p(~p)",
               [End - Start, ReplicationLeader,
                DestinationCluster, DestinationNode]),
    ?assertEqual(0,
                 repl_util:wait_for_reads(DestinationNode,
                                          Start,
                                          End,
                                          Bucket,
                                          1)).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, Bucket) ->
    write_to_cluster(Node, Start, End, Bucket, 1).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, Bucket, Quorum) ->
    lager:info("Writing ~p keys to node ~p in bucket ~p.", [End - Start + 1, Node, Bucket]),
    ?assertEqual([],
                 repl_util:do_write(Node, Start, End, Bucket, Quorum)).

%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Bucket, Errors) ->
    read_from_cluster(Node, Start, End, Bucket, Errors, 1).

%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Bucket, Errors, Quorum) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    Res2 = rt:systest_read(Node, Start, End, Bucket, Quorum, <<>>, true),
    ?assertEqual(Errors, length(Res2)).

%% @doc Assert we can perform one fullsync cycle, and that the number of
%%      expected failures is correct.
check_fullsync(Node, Cluster, ExpectedFailures) ->
    {Time, _} = timer:tc(repl_util,
                         start_and_wait_until_fullsync_complete,
                         [Node, Cluster]),
    lager:info("Fullsync completed in ~p seconds", [Time/1000/1000]),

    Status = rpc:call(Node, riak_repl_console, status, [quiet]),

    Props = case proplists:get_value(fullsync_coordinator, Status) of
        [{_Name, Props0}] ->
            Props0;
        Multiple ->
            {_Name, Props0} = lists:keyfind(Cluster, 1, Multiple),
            Props0
    end,

    %% check that the expected number of partitions failed to sync
    ErrorExits = proplists:get_value(error_exits, Props),
    lager:info("Error exits: ~p", [ErrorExits]),
    ?assertEqual(ExpectedFailures, ErrorExits),

    %% check that we retried each of them 5 times
    RetryExits = proplists:get_value(retry_exits, Props),
    lager:info("Retry exits: ~p", [RetryExits]),
    ?assert(RetryExits >= ExpectedFailures * 5),

    ok.

%% @doc Add an intercept on a target node to simulate a given failure
%%      mode, and then enable fullsync replication and verify completes
%%      a full cycle.  Subsequently reboot the node.
validate_intercepted_fullsync(InterceptTarget,
                              Intercept,
                              ReplicationLeader,
                              ReplicationCluster,
                              NumIndicies) ->
    lager:info("Validating intercept ~p on ~p.",
               [Intercept, InterceptTarget]),

    %% Add intercept.
    ok = rt_intercept:add(InterceptTarget, Intercept),

    %% Verify fullsync.
    ok = repl_util:check_fullsync(ReplicationLeader,
                                  ReplicationCluster,
                                  NumIndicies),

    %% Reboot node.
    rt:stop_and_wait(InterceptTarget),
    rt:start_and_wait(InterceptTarget),

    %% Wait for riak_kv and riak_repl to initialize.
    rt:wait_for_service(InterceptTarget, riak_kv),
    rt:wait_for_service(InterceptTarget, riak_repl),

    %% Wait until AAE trees are compueted on the rebooted node.
    rt:wait_until_aae_trees_built([InterceptTarget]).


%%
%% emit list of
%%
%%   {TimeStamp, {PartitionIndex, aae_fullsync_started}}
%%   {TimeStamp, {PartitionIndex, estimated_number_of_keys, EstimatedNymberOfKeys}}
%%   {TimeStamp, {PartitionIndex, finish_sending, BloomOrNot, BloomCount, DiffCount}}
%%   {TimeStamp, {PartitionIndex, aae_fullsync_completed}}
%%

-record(aae_stat, {
          start_time        :: erlang:timestamp(),
          estimate_time     :: erlang:timestamp(),
          direct_time       :: erlang:timestamp(),
          bloom_time        :: erlang:timestamp(),
          end_time          :: erlang:timestamp(),
          key_estimate = 0  :: non_neg_integer(),
          diff_count   = 0  :: non_neg_integer(),
          direct_count = 0  :: non_neg_integer(),
          bloom_count  = 0  :: non_neg_integer(),
          use_bloom = false :: boolean(),
          direct_mode       :: undefined | inline | buffered
         }).


get_aae_fullsync_activity() ->
    lager:info("Combing aae logs"),
    timer:sleep(2000),
    Logs = lists:append([ find_repl_logs(Log) || Log <- rt:get_node_logs() ]),
    Logs.

select_logs_in_time_interval(From,To,Logs) ->
    Duration = timer:now_diff(To,From),

    lager:info("Extracting logs from ~p to ~p", [From, To]),

    RelevantLogs = lists:foldl(fun(Entry={Time, _}, Acc) ->
                                       case timer:now_diff(Time,From) of
                                           T when T >= 0, T =< Duration ->
                                               [Entry|Acc];
                                           _ ->
                                               Acc
                                       end
                               end,
                               [],
                               Logs),
    RelevantLogs.

activate_debug_for_validate_aae_fullsync(Nodes) ->
    [rpc:call(Node, lager, set_loglevel, [lager_file_backend, "./log/console.log", debug]) ||
       Node <- Nodes].


%% From: Start timestamp used for pulling logs for analysis.
%% _To: End timestamp used for pulling logs for analysis. Currently not used because we look at every log statement after From.
%% NVal: The n-val fo the fullsync. When syncing with muliple n-vals, use the mean across all synced bucket weighed by NumKeys in the bucket.
%% QVal: Number of partitions synced
%% TotalKeys: The correct total number of keys in the source cluster. Used for checking if Riak's estimate is precise enough.
%% KeysChanged: The correct number of keys that differ between the clusters. Used for checking if AAE finds them all.
validate_aae_fullsync(FromTimestamp, _ToTimstamp, NVal, QVal, TotalKeys, KeysChanged) ->
    
    Partitions = get_partitions(FromTimestamp),

    FoundDiffs         = orddict:fold(fun(_, #aae_stat{ diff_count=N }, Acc) -> N+Acc end, 0, Partitions),
    TotalKeysEstimatedByRiak = orddict:fold(fun(_, #aae_stat{ key_estimate=N }, Acc) -> N+Acc end, 0, Partitions),
    PartitionsUsingBloomStrategyCount    = orddict:size( orddict:filter( fun(_, #aae_stat{ use_bloom=UseBloom }) -> UseBloom end, Partitions )),

    lager:info("AAE expected fullsync stats: partitions:~p, real number of keys:~p, diffs expected:~p", [QVal, trunc(TotalKeys * NVal), KeysChanged]),
    lager:info("AAE found    fullsync stats: partitions:~p, estimated number of keys :~p, diffs found:~p", [length(Partitions), TotalKeysEstimatedByRiak, FoundDiffs]),
    lager:info("AAE ~p partitions used bloom filter / fold", [PartitionsUsingBloomStrategyCount]),

    case QVal == orddict:size(Partitions) of
        true ->
            lager:info("OK - number of partitions: ~p", [QVal]);
        false ->
            lager:error("BAD - Wrong number of partitions in fullsync: ~p vs ~p", [QVal, ordsets:size(Partitions)])
    end,

    %% Calculate how far off the estimate is from the real value.
    EstimateSkew = TotalKeysEstimatedByRiak - (TotalKeys * NVal) ,
    EstSkewPercentage = EstimateSkew / TotalKeysEstimatedByRiak,

    case abs(EstSkewPercentage) =< 0.15 of
        true ->
            lager:info("OK - Estimate is ~p% off", [ EstSkewPercentage*100 ]);
        false ->
            lager:error("BAD - Estimate is ~p% off", [ EstSkewPercentage*100 ]),
            ?assert(false)
    end,


    DiffSkew = FoundDiffs - KeysChanged,
    case KeysChanged of
        0 ->
            case DiffSkew of
                0 ->
                    lager:info("OK - diff count is 0");
                _ ->
                    lager:info("BAD - diff count is ~p; should be 0", [FoundDiffs]),
                    ?assert(false)
            end;
        _ ->
            DiffSkewPercentage = DiffSkew / KeysChanged,
            case abs(DiffSkewPercentage) =< 0.05 of
                true ->
                    lager:info("OK - Diff count is ~p% off", [ DiffSkewPercentage*100 ]);
                false ->
                    lager:error("BAD - Diff count is ~p% off", [ DiffSkewPercentage*100 ]),
                    ?assert(false)
            end
    end,

    validate_partitions(Partitions, NVal, QVal, TotalKeys),
    ok.

get_partitions(From) ->

    Logs = get_aae_fullsync_activity(),
    RelevantLogs = select_logs_in_time_interval(From, {14110000,38604,655676}, Logs),

    Partitions =
        lists:foldl(fun({Time, {PI, aae_fullsync_started}}, Dict) ->
                            Dict2 = orddict:update( PI, fun(E)->E end, #aae_stat{}, Dict ),
                            orddict:update( PI,
                                            fun(Stat) -> Stat#aae_stat{ start_time=Time } end,
                                            Dict2);
                       ({Time, {PI, aae_fullsync_completed}}, Dict) ->
                            Dict2 = orddict:update( PI, fun(E)->E end, #aae_stat{}, Dict ),
                            orddict:update( PI,
                                            fun(Stat) -> Stat#aae_stat{ end_time=Time } end,
                                            Dict2);
                       ({Time, {PI, estimated_number_of_keys, Estimate}}, Dict) ->
                            Dict2 = orddict:update( PI, fun(E)->E end, #aae_stat{}, Dict ),
                            orddict:update( PI,
                                            fun(Stat) -> Stat#aae_stat{ key_estimate=Estimate, estimate_time=Time } end,
                                            Dict2);
                       ({Time, {PI, finish_sending, Bloom, BloomCount, PartDiffs}}, Dict) ->
                            Dict2 = orddict:update( PI, fun(E)->E end, #aae_stat{}, Dict ),
                            orddict:update( PI,
                                            fun(Stat) -> Stat#aae_stat{
                                                                       use_bloom=Bloom,
                                                                       bloom_count=BloomCount,
                                                                       diff_count=PartDiffs,
                                                                       bloom_time=Time
                                                                      } end,
                                            Dict2);
                       ({Time, {PI, aae_direct, Mode, Count}}, Dict) ->
                            Dict2 = orddict:update( PI, fun(E)->E end, #aae_stat{}, Dict ),
                            orddict:update( PI,
                                            fun(Stat) -> Stat#aae_stat{ direct_count=Count, direct_mode=Mode, direct_time=Time } end,
                                            Dict2);
                       (_, Acc) ->
                            Acc
                    end,
                    orddict:new(),
                    lists:sort(RelevantLogs)),
    Partitions.


validate_partitions(Partitions, NVal, QVal, TotalKeys) ->

    ExpectedKeys = (trunc(TotalKeys * NVal) div QVal),

    orddict:fold(fun(PI, #aae_stat{
                       key_estimate=KeyEst,

                       start_time=StartTime,
                       estimate_time=EstimateTime,
                       direct_time=DirectTime,
                       bloom_time=BloomTime,
                       end_time=EndTime,

                       diff_count=DiffCount,
                       direct_count=DirectCount,
                       direct_mode=DirectMode

                      }, ok) ->
                         lager:info("== AAE validating partition ~p ==", [PI]),

                         KeysOff = ((KeyEst - ExpectedKeys) / ExpectedKeys),
                         lager:info("key count is ~p% off", [KeysOff * 100]),

                         EstimateUSec = timer:now_diff(EstimateTime, StartTime),
                         ExchangeUSec = timer:now_diff(DirectTime, EstimateTime),
                         BufferedUSec = timer:now_diff(BloomTime, DirectTime),
                         BloomUSec    = timer:now_diff(EndTime,BloomTime),

                         (EstimateUSec == undefined) orelse
                             lager:info(" prepare time ~psec", [EstimateUSec / 1000000]),

                         case DirectMode of
                             buffered ->
                                 ((ExchangeUSec == undefined)) orelse
                                     lager:info("exchange time ~psec", [(ExchangeUSec) / 1000000]),
                                 ((BufferedUSec == undefined) or (DirectCount == 0)) orelse
                                     lager:info("buffered time ~pusec/diff", [(BufferedUSec) / DirectCount]);
                             inline ->
                                 ((ExchangeUSec == undefined) or (BufferedUSec == undefined)) orelse
                                     lager:info("exchange time ~psec", [(ExchangeUSec+BufferedUSec) / 1000000])
                         end,

                         ((BloomTime == undefined) or (BloomUSec == undefined)) orelse
                             lager:info("   bloom time ~psec", [BloomUSec / 1000000]),

                         (DirectCount == 0) orelse
                             lager:info("exchange time ~pusec/diff for ~p diffs (~p sent ~p)",
                                        [(ExchangeUSec+BufferedUSec) div DiffCount, DiffCount, DirectCount, DirectMode]),

                         ((BloomUSec == undefined) or ((DiffCount-DirectCount) == 0)) orelse
                             lager:info("   bloom time ~pusec/diff for ~p objects", [BloomUSec div (DiffCount-DirectCount), (DiffCount-DirectCount)]),

                         ok
                 end,
                 ok,
                 Partitions).


find_repl_logs({Path, Port}) ->
    case re:run(Path, "console\.log$") of
        {match, _} ->
            match_loop(Port, file:read_line(Port), aae_fullsync_patterns(), []);
        nomatch ->
            %% save time not looking through other logs
            []
    end.

match_loop(Port, {ok, Data}, Matchers, Acc) ->
    case lists:foldl(fun({Patt,Fun}, {next, Data2}) ->
                             case re:run(Data2, Patt, [{capture, all_but_first, list}]) of
                                 {match, Match} ->
                                     {done, Fun(Match)};
                                 nomatch ->
                                     {next, Data2}
                             end;
                        (_, {done, Value}) ->
                             {done, Value}
                     end,
                     {next, Data},
                     Matchers) of
        {done, Value} ->
            {ok, TimeStamp} = extract_timestamp(Data),
            match_loop(Port, file:read_line(Port), Matchers, [{TimeStamp, Value}|Acc]);
        {next, _} ->
            match_loop(Port, file:read_line(Port), Matchers, Acc)
    end;
match_loop(_, _, _, Acc) ->
    lists:reverse(Acc).

extract_timestamp(Data) ->
    Re = "^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{3})",
    case re:run(Data, Re, [{capture, all_but_first, list}]) of
        {match, DateMatch} ->
            [Y,M,D, Hr,Min,Sec, Millis] = [ list_to_integer(E) || E <- DateMatch ],
            [UTC] = calendar:local_time_to_universal_time_dst({{Y,M,D}, {Hr,Min,Sec}}),
            GregorianSeconds = calendar:datetime_to_gregorian_seconds(UTC) - 62167219200,
            TimeStamp = {GregorianSeconds div 1000000, GregorianSeconds rem 1000000, Millis * 1000},
            {ok, TimeStamp};
        nomatch ->
            error
    end.

aae_fullsync_patterns() ->
    [ { "EstimatedNrKeys ([0-9]*) for partition ([0-9]+)",
        fun([NumKeys, PartIndex]) ->
                {list_to_integer(PartIndex), estimated_number_of_keys, list_to_integer(NumKeys)}
        end },

      { "No Bloom folding over ([0-9]+)/([0-9]+) differences for partition ([0-9]+) with EstimatedNrKeys",
        fun([BloomCount, DiffCount, PartIndex]) ->
                {list_to_integer(PartIndex), finish_sending, false, list_to_integer(BloomCount), list_to_integer(DiffCount)}
        end },

      { "Bloom folding over ([0-9]+)/([0-9]+) differences for partition ([0-9]+) with EstimatedNrKeys",
        fun([BloomCount, DiffCount, PartIndex]) ->
                { list_to_integer(PartIndex), finish_sending, true, list_to_integer(BloomCount), list_to_integer(DiffCount)}
        end },

      { "AAE fullsync source completed partition ([0-9]+)",
        fun([PartIndex]) ->
                {list_to_integer(PartIndex), aae_fullsync_completed }
        end },

      { "AAE fullsync source worker started for partition ([0-9]+)",
        fun([PartIndex]) ->
                { list_to_integer(PartIndex), aae_fullsync_started }
        end },

      { "Directly sent ([0-9]+) differences inline for partition ([0-9]+)",
        fun([DirectCount,PartIndex]) ->
                { list_to_integer(PartIndex), aae_direct, inline, list_to_integer(DirectCount) }
        end },

      { "Directly sending ([0-9]+) differences for partition ([0-9]+)",
        fun([DirectCount,PartIndex]) ->
                { list_to_integer(PartIndex), aae_direct, buffered, list_to_integer(DirectCount) }
        end }

      ].


update_props(DefaultProps, NewProps, Node, Nodes, Bucket) ->
    lager:info("Setting bucket properties ~p for bucket ~p on node ~p",
               [NewProps, Bucket, Node]),
    rpc:call(Node, riak_core_bucket, set_bucket, [Bucket, NewProps]),
    rt:wait_until_ring_converged(Nodes),

    UpdatedProps = get_current_bucket_props(Nodes, Bucket),
    ?assertNotEqual(DefaultProps, UpdatedProps).

%% fetch bucket properties via rpc
%% from a node or a list of nodes (one node is chosen at random)
get_current_bucket_props(Nodes, Bucket) when is_list(Nodes) ->
    Node = lists:nth(length(Nodes), Nodes),
    get_current_bucket_props(Node, Bucket);
get_current_bucket_props(Node, Bucket) when is_atom(Node) ->
    rpc:call(Node,
             riak_core_bucket,
             get_bucket,
             [Bucket]).

create_clusters_with_rt(ClusterSetup, Direction) ->
    [ANodes, BNodes] = rt:build_clusters(ClusterSetup),
    setup_cluster_rt([ANodes, BNodes], Direction).

deploy_clusters_with_rt(ClusterSetup, Direction) ->
    [ANodes, BNodes] = rt:deploy_clusters(ClusterSetup),
    setup_cluster_rt([ANodes, BNodes], Direction).

setup_cluster_rt([ANodes, BNodes], Direction) ->
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)),
    case Direction of
        '<->' ->
            setup_rt(ANodes, '->', BNodes), setup_rt(ANodes, '<-', BNodes);
        _ ->
            setup_rt(ANodes, Direction, BNodes)
        end,
    {ANodes, BNodes}.

setup_rt(ANodes, '->', BNodes) ->
    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    %% A -> B
    connect_clusters(AFirst, BFirst),
    repl_util:enable_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),
    repl_util:start_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes));

setup_rt(ANodes, '<-', BNodes) ->
    AFirst = hd(ANodes),
    BFirst = hd(BNodes),
    %% B -> A
    connect_clusters(BFirst, AFirst),
    repl_util:enable_realtime(BFirst, "A"),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)),
    repl_util:start_realtime(BFirst, "A"),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)).

verify_correct_connection(ANodes, BNodes) when length(ANodes) == length(BNodes) ->
    verify_correct_connection(ANodes),
    verify_correct_connection(BNodes),
    lager:info("verify_correct_connection ok", []);

verify_correct_connection(ANodes, BNodes) when length(ANodes) > length(BNodes) ->
    verify_correct_connection(BNodes),
    NumOfBNodes = length(BNodes),
    verify_for_smaller_sink_cluster(ANodes, NumOfBNodes),
    lager:info("verify_correct_connection ok", []);

verify_correct_connection(ANodes, BNodes) when length(ANodes) < length(BNodes) ->
    verify_correct_connection(ANodes),
    NumOfANodes = length(ANodes),
    verify_for_smaller_sink_cluster(BNodes, NumOfANodes),
    lager:info("verify_correct_connection ok", []).

%% Can be used for replications with same number of nodes in both clusters
%% or if local cluster have less number of nodes
verify_correct_connection(Nodes) ->
    ConnectionList = lists:sort([rt_source_connected_to(Node) || Node <- Nodes]),
    ?assertEqual([], ConnectionList -- lists:sort(sets:to_list(sets:from_list(ConnectionList)))).

verify_for_smaller_sink_cluster(Nodes, SinkNodes) ->
    %% Get IP port of remote nodes
    ConnectionList = lists:sort([rt_source_connected_to(Node) || Node <- Nodes]),
    %% Get unique IP port list
    RemoteUsed = lists:sort(sets:to_list(sets:from_list(ConnectionList))),
    %% Verify that all remote nodes are used.
    ?assertEqual(SinkNodes, length(RemoteUsed)),


    %% Get list with use count of remote IP port.
    ConnectionsNrUsed =
        [length([ok || Connection <- ConnectionList, Connection == Remote]) || Remote  <- RemoteUsed],
    %% Verify that it's not uneven.
    ?assert((lists:max(ConnectionsNrUsed) - lists:min(ConnectionsNrUsed)) =< 1).

rt_source_connected_to(Node) ->
    %%[{sources,[{source_stats,[{socket,[{peername,"127.0.0.1:10066"},{sockname,"127.0.0.1:53307"}]}
    [{sources, List1}] = rpc:call(Node, riak_repl_console, server_stats, []),
    {source_stats, List2} = lists:keyfind(source_stats, 1, List1),
    {rt_source_connected_to, List3} = lists:keyfind(rt_source_connected_to, 1, List2),
    {socket, List4} = lists:keyfind(socket, 1, List3),
    {peername, List5} = lists:keyfind(peername, 1, List4),
    List5.

%% @doc Connect two clusters for replication using their respective
%%      leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, IP, Port).
