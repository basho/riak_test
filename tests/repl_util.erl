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
         wait_until_aae_trees_built/1,
         wait_for_reads/5,
         start_and_wait_until_fullsync_complete/1,
         start_and_wait_until_fullsync_complete/2,
         connect_cluster/3,
         disconnect_cluster/2,
         wait_for_connection/2,
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
         get_port/1,
         get_leader/1,
         write_to_cluster/4,
         read_from_cluster/5,
         check_fullsync/3,
         validate_completed_fullsync/6
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
                UniqueLeaders = lists:usort(
                                  lists:filter(leader_result_filter_fun(),
                                               LeaderResults)),
                length(UniqueLeaders) == 1
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
        end). %% 40 seconds is enough for repl

wait_until_no_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(connected_clusters, Status) of
                    [] ->
                        true;
                    _ ->
                        false
                end
        end). %% 40 seconds is enough for repl

wait_for_reads(Node, Start, End, Bucket, R) ->
    rt:wait_until(Node,
        fun(_) ->
                Reads = rt:systest_read(Node, Start, End, Bucket, R),
                Reads == []
        end),
    Reads = rt:systest_read(Node, Start, End, Bucket, R),
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
    Status0 = rpc:call(Node, riak_repl_console, status, [quiet]),
    Count0 = proplists:get_value(server_fullsyncs, Status0),
    Count = case Cluster of
                undefined ->
                    %% count the # of fullsync enabled clusters
                    Count0 + length(string:tokens(proplists:get_value(fullsync_enabled,
                                                        Status0), ", "));
                _ ->
                    Count0 + 1
            end,
    lager:info("waiting for fullsync count to be ~p", [Count]),

    lager:info("Starting fullsync on ~p (~p)", [Node,
            rtdev:node_version(rtdev:node_id(Node))]),
    Args = case Cluster of
               undefined ->
                   ["start"];
               _ ->
                   ["start", Cluster]
           end,
    rpc:call(Node, riak_repl_console, fullsync, [Args]),
    %% sleep because of the old bug where stats will crash if you call it too
    %% soon after starting a fullsync
    timer:sleep(500),

    Res = rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(server_fullsyncs, Status) of
                    C when C >= Count ->
                        true;
                    _ ->
                        false
                end
        end),
    ?assertEqual(ok, Res),

    lager:info("Fullsync on ~p complete", [Node]).

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
                                Pid ! {self(), status},
                                receive
                                    {Pid, status, _} ->
                                        true;
                                    {Pid, connecting, _} ->
                                        false
                                end
                        end;
                    _ ->
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

%% AAE support
wait_until_aae_trees_built(Cluster) ->
    lager:info("Check if all trees built for nodes ~p", [Cluster]),
    F = fun(Node) ->
            Info = rpc:call(Node,
                            riak_kv_entropy_info,
                            compute_tree_info,
                            []),
            NotBuilt = [X || {_,undefined}=X <- Info],
            NotBuilt == []
    end,
    [rt:wait_until(Node, F) || Node <- Cluster],
    ok.

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
    lager:info("Writing ~p keys to node ~p.", [End - Start, Node]),
    ?assertEqual([],
                 repl_util:do_write(Node, Start, End, Bucket, 1)).

%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Bucket, Errors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start, Node]),
    Res2 = rt:systest_read(Node, Start, End, Bucket, 1),
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
