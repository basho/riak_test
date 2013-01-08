-module(replication2_dirty).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

confirm() ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,
    _FullsyncOnly = <<TestHash/binary, "-fullsync_only">>,
    _RealtimeOnly = <<TestHash/binary, "-realtime_only">>,
    _NoRepl = <<TestHash/binary, "-no_repl">>,

    NumNodes = 6,
    ClusterASize = list_to_integer(replication2:get_os_env("CLUSTER_A_SIZE", "3")),
    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    Nodes = deploy_nodes(NumNodes, Conf),
    {[AFirst|_] = ANodes, [BFirst|_] = BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    replication2:make_cluster(ANodes),

    lager:info("Build cluster B"),
    replication2:make_cluster(BNodes),

    replication2:name_cluster(AFirst, "A"),
    replication2:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    %% get the leader for the first cluster
    replication2:wait_until_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
    %LeaderB = rpc:call(BFirst, riak_core_cluster_mgr, get_leader, []),

    {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                                 [riak_core, cluster_mgr]),
    replication2:connect_cluster(LeaderA, "127.0.0.1", Port),

    ?assertEqual(ok, replication2:wait_for_connection(LeaderA, "B")),
    replication2:enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    replication2:start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    replication2:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),

    % nothing should be dirty initially
    lager:info("Waiting until all nodes clean"),
    wait_until_all_nodes_clean(LeaderA),

    %% write some data on A
    ?assertEqual(ok, replication2:wait_for_connection(LeaderA, "B")),
    %io:format("~p~n", [rpc:call(LeaderA, riak_repl_console, status, [quiet])]),
    lager:info("Writing 2000 more keys to ~p", [LeaderA]),
    ?assertEqual([], replication2:do_write(LeaderA, 101, 2000, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 2000 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, replication2:wait_for_reads(BFirst, 101, 2000, TestBucket, 2)),

    [ ?assertEqual(0, get_dirty_stat(Node)) || Node <- ANodes],
    [ ?assertEqual(0, get_dirty_stat(Node)) || Node <- BNodes],
    [ ?assertEqual({0,0}, get_rt_errors(Node)) || Node <- ANodes],
    [ ?assertEqual({0,0}, get_rt_errors(Node)) || Node <-BNodes],

    lager:info("Waiting until all nodes clean"),
    wait_until_all_nodes_clean(LeaderA),

    lager:info("Manually setting rt_dirty state"),

    % manually set this for now to simulate source errors
    Result = rpc:call(LeaderA, riak_repl_stats, rt_source_errors, []),
    lager:info("Result = ~p", [Result]),

    lager:info("Waiting until dirty"),
    wait_until_coord_has_dirty(LeaderA),

    lager:info("Starting fullsync"),
    replication2:start_and_wait_until_fullsync_complete(LeaderA),
    lager:info("Wait for all nodes to show up clean"),
    wait_until_all_nodes_clean(LeaderA),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    lager:info("Multiple node test"),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% test multiple nodes dirty
    [DirtyA , DirtyB | _] = ANodes,
    % manually set this for now to simulate source errors
    ResultA = rpc:call(DirtyA, riak_repl_stats, rt_source_errors, []),
    ResultB = rpc:call(DirtyB, riak_repl_stats, rt_sink_errors, []),
    lager:info("Result = ~p", [ResultA]),
    lager:info("Result = ~p", [ResultB]),

    lager:info("Waiting until dirty"),
    wait_until_coord_has_dirty(DirtyA),
    wait_until_coord_has_dirty(DirtyB),

    lager:info("Starting fullsync"),
    replication2:start_and_wait_until_fullsync_complete(LeaderA),
    lager:info("Wait for all nodes to show up clean"),
    wait_until_all_nodes_clean(LeaderA),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    lager:info("Multiple node test, one failed during fullsync"),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% test multiple nodes dirty
    [DirtyC , DirtyD | _] = ANodes,
    % manually set this for now to simulate source errors
    ResultC = rpc:call(DirtyC, riak_repl_stats, rt_source_errors, []),
    lager:info("ResultC = ~p", [ResultC]),

    lager:info("Waiting until dirty"),
    wait_until_coord_has_dirty(DirtyC),

    lager:info("Starting fullsync"),
    spawn(fun() ->
                timer:sleep(1000),
                lager:info("Marking node as dirty during a fullsync"),
                ResultC = rpc:call(DirtyD, riak_repl_stats, rt_source_errors, []),
                lager:info("Result = ~p", [ResultC])
           end),
    replication2:start_and_wait_until_fullsync_complete(LeaderA),

    lager:info("Checking to see if C is still clean"),
    wait_until_node_clean(DirtyC),
    lager:info("Checking to see if D is still dirty"),
    wait_until_coord_has_dirty(DirtyD),

    % Clear out all dirty state
    replication2:start_and_wait_until_fullsync_complete(LeaderA),


    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    lager:info("Brutally kill the sink nodes"),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    spawn(fun() ->
                timer:sleep(1000),
                [rt:brutal_kill(Node) || Node <- BNodes]
          end),
    % 3000 may need to be increased if the test fails here on
    % a fast machine
    lager:info("Writing 3000 more keys to ~p", [LeaderA]),
    ?assertEqual([], replication2:do_write(LeaderA, 0, 3000, TestBucket, 2)),

    wait_until_coord_has_any_dirty(LeaderA),


    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    lager:info("Check rt_dirty state after shutdown"),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    [ rt:stop_and_wait(Node) || Node <- ANodes],
    [ rt:start_and_wait(Node) || Node <- ANodes],
    wait_until_coord_has_any_dirty(LeaderA),

    pass.

get_dirty_stat(Node) ->
    Stats = rpc:call(Node, riak_repl_stats, get_stats, []),
    %lager:info("RT_DIRTY = ~p", [proplists:get_value(rt_dirty, Stats, -1)]),
    proplists:get_value(rt_dirty, Stats, -1).

get_rt_errors(Node) ->
    Stats = rpc:call(Node, riak_repl_stats, get_stats, []),
    SourceErrors = proplists:get_value(rt_source_errors, Stats, -1),
    SinkErrors = proplists:get_value(rt_sink_errors, Stats, -1),
    lager:info("Source errors = ~p, sink errors = ~p", [SourceErrors, SinkErrors]),
    {SourceErrors, SinkErrors}.

wait_until_coord_has_dirty(Node) ->
    Res = rt:wait_until(Node,
                        fun(_) ->
                    lager:info("Checking dirty for node ~p", [Node]),
                    Status = rpc:call(Node, riak_repl2_fscoordinator, status, []),
                    case Status of
                        {badrpc, _} -> false;
                        [] -> false;
                        [{_,Stats}|_Rest] ->
                            NodeString = proplists:get_value(fullsync_suggested, Stats),
                            Nodes = string:tokens(NodeString,","),
                            lager:info("Nodes = ~p",[Nodes]),
                            lists:member(erlang:atom_to_list(Node), Nodes)
                    end
            end),
    ?assertEqual(ok, Res).

wait_until_coord_has_any_dirty(SourceLeader) ->
    Res = rt:wait_until(SourceLeader,
                        fun(_) ->
                    lager:info("Checking for any dirty nodes"),
                    Status = rpc:call(SourceLeader, riak_repl2_fscoordinator, status, []),
                    case Status of
                        {badrpc, _} -> false;
                        [] -> false;
                        [{_,Stats}|_Rest] ->
                            NodeString = proplists:get_value(fullsync_suggested, Stats),
                            Nodes = string:tokens(NodeString,","),
                            lager:info("Nodes = ~p",[Nodes]),
                            length(Nodes) > 0
                    end
            end),
    ?assertEqual(ok, Res).

%% yeah yeah, copy paste, I know
wait_until_node_clean(Node) ->
    Res = rt:wait_until(Node,
                        fun(_) ->
                    lager:info("Checking dirty for node ~p", [Node]),
                    Status = rpc:call(Node, riak_repl2_fscoordinator, status, []),
                    case Status of
                        {badrpc, _} -> false;
                        [] -> false;
                        [{_,Stats}|_Rest] ->
                            NodeString = proplists:get_value(fullsync_suggested, Stats),
                            Nodes = string:tokens(NodeString,","),
                            lager:info("Nodes = ~p",[Nodes]),
                            not lists:member(erlang:atom_to_list(Node), Nodes)
                    end
            end),
    ?assertEqual(ok, Res).

wait_until_all_nodes_clean(Leader) ->
    Res = rt:wait_until(Leader,
                        fun(L) ->
                    lager:info("Checking for all nodes clean"),
                    Status = rpc:call(L, riak_repl2_fscoordinator, status, []),
                    case Status of
                        {badrpc, _} -> false;
                        [] -> true;
                        [{_,Stats}|_Rest] ->
                            NodeString = proplists:get_value(fullsync_suggested, Stats),
                            Nodes = string:tokens(NodeString,","),
                            lager:info("Nodes = ~p",[Nodes]),
                            Nodes == []

                    end
            end),
    ?assertEqual(ok, Res).


