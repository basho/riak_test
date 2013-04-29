-module(replication2_fsschedule).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This tests fullsync scheduling in 1.2 and 1.3 Advanced Replication
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup_repl_clusters(Conf) ->
    NumNodes = 6,
    lager:info("Deploy ~p nodes", [NumNodes]),

    Nodes = deploy_nodes(NumNodes, Conf),

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

    %% write some data on A
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    repl_util:enable_fullsync(LeaderA, "B"),
    repl_util:enable_fullsync(LeaderA, "C"),
    rt:wait_until_ring_converged(ANodes),
    {LeaderA, ANodes, BNodes, CNodes, Nodes}.


test_multiple_schedules() ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,

    Conf = [
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval, [{"B",1}, {"C",2}]}
             ]}
           ],
    {LeaderA, _ANodes, _BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf),
    rt:log_to_nodes(AllNodes, "Test multiple fullsync schedules from A -> [B,C]"),

    lager:info("Writing 500 keys to ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 0, 500, TestBucket, 1)),

    Status0 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),
    Count = proplists:get_value(server_fullsyncs, Status0),
    ?assertEqual(0, Count),

    Start = riak_core_util:moment(),
    lager:info("Note: Waiting for fullsyncs can take several minutes"),
    wait_until_n_bnw_fullsyncs(LeaderA, "B", 3),
    Finish = riak_core_util:moment(),
    Diff = Finish - Start,
    Minutes = Diff / 60,
    %% Why 5? 1 minute for repl to B to start, 3 fullsyncs + room for slow boxes
    ?assert(Minutes =< 5),

    {_AFirst, BFirst, CFirst} = get_firsts(AllNodes),
    %% verify data is replicated to B
    lager:info("Reading 500 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 0, 500, TestBucket, 2)),
    %% verify data is replicated to C
    lager:info("Reading 500 keys written to ~p from ~p", [LeaderA, CFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(CFirst, 0, 500, TestBucket, 2)),

    FSCountToC = get_cluster_fullsyncs(LeaderA, "C"),
    %% Why 2? 1 minute for repl to C to start, 1 fullsync
    ?assert(FSCountToC =< 2),
    rt:clean_cluster(AllNodes),
    pass.

test_single_schedule() ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,

    Conf = [
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval, 1}
             ]}
           ],
    {LeaderA, _ANodes, _BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf),
    rt:log_to_nodes(AllNodes, "Test single fullsync schedule from A -> [B,C]"),

    lager:info("Writing 500 keys to ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 0, 500, TestBucket, 1)),

    Status0 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),
    Count = proplists:get_value(server_fullsyncs, Status0),
    ?assertEqual(0, Count),

    Start = riak_core_util:moment(),
    lager:info("Note: Waiting for fullsyncs can take several minutes"),
    wait_until_n_bnw_fullsyncs(LeaderA, "B", 3),
    Finish = riak_core_util:moment(),
    Diff = Finish - Start,
    Minutes = Diff / 60,
    ?assert(Minutes =< 5 andalso Minutes >= 3),

    {_AFirst, BFirst, CFirst} = get_firsts(AllNodes),
    %% verify data is replicated to B
    lager:info("Reading 500 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 0, 500, TestBucket, 2)),

    %% verify data is replicated to C
    lager:info("Reading 500 keys written to ~p from ~p", [LeaderA, CFirst]),
    ?assertEqual(0, repl_util:wait_for_reads(CFirst, 0, 500, TestBucket, 2)),

    FSCountToC = get_cluster_fullsyncs(LeaderA, "C"),
    %% Why 2? 1 minute for repl to C to start, 1 fullsync
    ?assert(FSCountToC =< 5 andalso FSCountToC >= 3),
    rt:clean_cluster(AllNodes),
    pass.


test_mixed_12_13() ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,

    Conf = [
            {riak_repl,
             [
              {fullsync_on_connect, false},
              {fullsync_interval, 1}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf),

    {AFirst, BFirst, _CFirst} = get_firsts(AllNodes),

    repl_util:wait_until_leader_converge(ANodes),
    repl_util:wait_until_leader_converge(BNodes),
    repl_util:wait_until_leader_converge(CNodes),

    lager:info("Writing 500 keys to ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 0, 500, TestBucket, 1)),

    lager:info("Adding repl listener to cluster A"),
    ListenerArgs = [[atom_to_list(LeaderA), "127.0.0.1", "9010"]],
    Res = rpc:call(LeaderA, riak_repl_console, add_listener, ListenerArgs),
    ?assertEqual(ok, Res),

    lager:info("Adding repl site to cluster B"),
    SiteArgs = ["127.0.0.1", "9010", "rtmixed"],
    Res = rpc:call(BFirst, riak_repl_console, add_site, [SiteArgs]),

    lager:info("Waiting until scheduled fullsync occurs. Go grab a beer, this may take awhile."),

    wait_until_n_bnw_fullsyncs(LeaderA, "B", 3),
    wait_until_n_bnw_fullsyncs(LeaderA, "C", 3),
    %% 1.3 fullsyncs increment the 1.2 fullsync counter, backwards
    %% compatability is a terrible thing
    wait_until_12_fs_complete(LeaderA, 9),

    Status0 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),
    Count0 = proplists:get_value(server_fullsyncs, Status0),
    FS_B = get_cluster_fullsyncs(AFirst, "B"),
    FS_C = get_cluster_fullsyncs(AFirst, "C"),
    %% count the actual 1.2 fullsyncs
    Count = Count0 - (FS_B + FS_C),

    lager:info("1.2 Count = ~p", [Count]),
    lager:info("1.3 B Count = ~p", [FS_B]),
    lager:info("1.3 C Count = ~p", [FS_C]),

    ?assert(Count >= 3 andalso Count =< 6),
    ?assert(FS_B >= 3 andalso FS_B =< 6),
    ?assert(FS_C >= 3 andalso FS_C =< 6),
    pass.


confirm() ->
    AllTests = [test_multiple_schedules(), test_single_schedule(), test_mixed_12_13()],
    case lists:all(fun (Result) -> Result == pass end, AllTests) of
        true ->  pass;
        false -> sadtrombone
    end.

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
                                                % let it fail if keys are missing
    ClusterData = proplists:get_value(ClusterName, Status),
    proplists:get_value(fullsyncs_completed, ClusterData).

wait_until_n_bnw_fullsyncs(Node, DestCluster, N) ->
    lager:info("Waiting for fullsync count for ~p to be ~p", [DestCluster, N]),
    Res = rt:wait_until(Node,
        fun(_) ->
                Fullsyncs = get_cluster_fullsyncs(Node, DestCluster),
                case Fullsyncs of
                    C when C >= N ->
                        true;
                    _Other ->
                        %% keep this in for tracing
                        %%lager:info("Total fullsyncs = ~p", [Other]),
                        %% sleep a while so the default 3 minute time out
                        %% doesn't screw us
                        timer:sleep(20000),
                        false
                end
        end),
    ?assertEqual(ok, Res),
    lager:info("Fullsync on ~p complete", [Node]).
