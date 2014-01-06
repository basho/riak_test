-module(replication2_fsschedule).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This tests fullsync scheduling in 1.4+ Advanced Replication%% intercept
%% gets called w/ v3 test too, let it
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup_repl_clusters(Conf, InterceptSetup) ->
    NumNodes = 6,
    lager:info("Deploy ~p nodes", [NumNodes]),

    Nodes = deploy_nodes(NumNodes, Conf),
    InterceptSetup(Nodes),

    lager:info("Nodes = ~p", [Nodes]),
    {[AFirst|_] = ANodes, Rest} = lists:split(2, Nodes),
    {[BFirst|_] = BNodes, [CFirst|_] = CNodes} = lists:split(2, Rest),

    %%AllNodes = ANodes ++ BNodes ++ CNodes,
    rt:log_to_nodes(Nodes, "Starting replication2_fullsync test"),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),
    lager:info("CNodes: ~p", [CNodes]),

    rt:log_to_nodes(Nodes, "Building and connecting Clusters"),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    lager:info("Build cluster C"),
    repl_util:make_cluster(CNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),
    repl_util:name_cluster(CFirst, "C"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    %% set the fullsync limits higher, so fullsyncs don't take forever
    [begin
                rpc:call(N, riak_repl_console, max_fssource_cluster,
                    [["10"]]),
                rpc:call(N, riak_repl_console, max_fssource_node, [["5"]]),
                rpc:call(N, riak_repl_console, max_fssink_node, [["5"]])
        end || N <- [AFirst, BFirst, CFirst]],

    %% get the leader for the first cluster
    repl_util:wait_until_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    {ok, {_IP, BPort}} = rpc:call(BFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", BPort),

    {ok, {_IP, CPort}} = rpc:call(CFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", CPort),


    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "C")),
    rt:wait_until_ring_converged(ANodes),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    repl_util:enable_fullsync(LeaderA, "B"),
    repl_util:enable_fullsync(LeaderA, "C"),
    rt:wait_until_ring_converged(ANodes),
    {LeaderA, ANodes, BNodes, CNodes, Nodes}.


test_multiple_schedules() ->
    Conf = [
            {riak_core, [{ring_creation_size, 4}]},
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval, [{"B",1}, {"C",2}]}
             ]}
           ],
    {LeaderA, _ANodes, _BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf, fun install_v3_intercepts/1),
    lager:info("Waiting for fullsyncs"),
    wait_until_fullsyncs(LeaderA, "B", 5),
    wait_until_fullsyncs(LeaderA, "C", 5),
    rt:clean_cluster(AllNodes),
    pass.

test_single_schedule() ->
    Conf = [
            {riak_core, [{ring_creation_size, 4}]},
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval, 99}
             ]}
           ],
    {LeaderA, _ANodes, _BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf, fun install_v3_intercepts/1),
    rt:log_to_nodes(AllNodes, "Test shared fullsync schedule from A -> [B,C]"),
    %% let some msgs queue up, doesn't matter how long we wait
    lager:info("Waiting for fullsyncs"),
    wait_until_fullsyncs(LeaderA, "B", 10),
    wait_until_fullsyncs(LeaderA, "C", 10),
    rt:clean_cluster(AllNodes),
    pass.

test_mixed_12_13() ->
    Conf = [
            {riak_core, [{ring_creation_size, 4}]},
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval, 99}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, fun install_mixed_intercepts/1),

    {_AFirst, BFirst, _CFirst} = get_firsts(AllNodes),

    repl_util:wait_until_leader_converge(ANodes),
    repl_util:wait_until_leader_converge(BNodes),
    repl_util:wait_until_leader_converge(CNodes),

    lager:info("Adding repl listener to cluster A"),
    ListenerArgs = [[atom_to_list(LeaderA), "127.0.0.1", "9010"]],
    Res = rpc:call(LeaderA, riak_repl_console, add_listener, ListenerArgs),
    ?assertEqual(ok, Res),

    lager:info("Adding repl site to cluster B"),
    SiteArgs = ["127.0.0.1", "9010", "rtmixed"],
    Res = rpc:call(BFirst, riak_repl_console, add_site, [SiteArgs]),

    lager:info("Waiting for v2 repl to catch up. Good time to light up a cold can of Tab."),
    wait_until_fullsyncs(LeaderA, "B", 3),
    wait_until_fullsyncs(LeaderA, "C", 3),
    wait_until_12_fs_complete(LeaderA, 9),
    rt:clean_cluster(AllNodes),
    pass.


confirm() ->
    AllTests = [test_mixed_12_13(), test_multiple_schedules(), test_single_schedule()],
    case lists:all(fun (Result) -> Result == pass end, AllTests) of
        true ->  pass;
        false -> sadtrombone
    end.

wait_until_fullsyncs(Node, ClusterName, N) ->
    Res = rt:wait_until(Node,
        fun(_) ->
                FS = get_cluster_fullsyncs(Node, ClusterName),
                case FS of
                    {badrpc, _} ->
                        false;
                    undefined ->
                        false;
                    X when X >= N ->
                        true;
                    _ ->
                        false
                end
        end),
    ?assertEqual(ok, Res).

wait_until_12_fs_complete(Node, N) ->
    rt:wait_until(Node,
                  fun(_) ->
                          Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                          case proplists:get_value(server_fullsyncs, Status) of
                              C when C >= N ->
                                  true;
                              _ ->
                                  false
                          end
                  end).

get_firsts(Nodes) ->
    {[AFirst|_] = _ANodes, Rest} = lists:split(2, Nodes),
    {[BFirst|_] = _BNodes, [CFirst|_] = _CNodes} = lists:split(2, Rest),
    {AFirst, BFirst, CFirst}.

get_cluster_fullsyncs(Node, ClusterName) ->
    Status = rpc:call(Node, riak_repl2_fscoordinator, status, []),
    case proplists:lookup(ClusterName, Status) of
        none -> 0;
        {_, ClusterData} ->
            case proplists:lookup(fullsyncs_completed, ClusterData) of
                none -> 0;
                FSC -> FSC
            end
    end.

%% skip v2 repl interval checks
install_v3_intercepts(Nodes) ->
    [rt_intercept:add(Node, {riak_repl_util, [{{start_fullsync_timer,3},
                                                interval_check_v3}
                                                ]})
               || Node <- Nodes].

%% check v2 + v3 intervals
install_mixed_intercepts(Nodes) ->
     [rt_intercept:add(Node, {riak_repl_util, [{{start_fullsync_timer,3},
                                                interval_check_v3},
                                               {{schedule_fullsync,1},
                                                interval_check_v2}]})
             || Node <- Nodes].

