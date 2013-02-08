-module(replication2_fullsync).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

confirm() ->
    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,

    NumNodes = 6,
    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, [{"B",1}, {"C",2}]}
             ]}
    ],

    Nodes = deploy_nodes(NumNodes, Conf),
    lager:info("Nodes = ~p", [Nodes]),
    {[AFirst|_] = ANodes, Rest} = lists:split(2, Nodes),
    {[BFirst|_] = BNodes, [CFirst|_] = CNodes} = lists:split(2, Rest),

    AllNodes = ANodes ++ BNodes ++ CNodes,
    rt:log_to_nodes(AllNodes, "Starting replication2_fullsync test"),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),
    lager:info("CNodes: ~p", [CNodes]),

    rt:log_to_nodes(AllNodes, "Building and connecting Clusters"),

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
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),


    %% write some data on A
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    %io:format("~p~n", [rpc:call(LeaderA, riak_repl_console, status, [quiet])]),
    lager:info("Writing 2000 keys to ~p", [LeaderA]),
    ?assertEqual([], repl_util:do_write(LeaderA, 0, 2000, TestBucket, 1)),

    Status0 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),
    Count = proplists:get_value(server_fullsyncs, Status0),
    ?assertEqual(0, Count),


    rt:log_to_nodes(AllNodes, "Test fullsync schedule from A -> B"),

    Start = erlang:now(),
    wait_until_n_fullsyncs(LeaderA, 3),
    Finish = erlang:now(),
    Diff = timer:now_diff(Finish, Start),
    lager:info("Diff = ~p microseconds", [Diff]),


%    %% verify data is replicated to B
%    lager:info("Reading 2000 keys written to ~p from ~p", [LeaderA, BFirst]),
%    ?assertEqual(0, repl_util:wait_for_reads(BFirst, 101, 2000, TestBucket, 2)),
%
%    %% verify data is replicated to B
%    lager:info("Reading 2000 keys written to ~p from ~p", [LeaderA, CFirst]),
%    ?assertEqual(0, repl_util:wait_for_reads(CFirst, 101, 2000, TestBucket, 2)),

    pass.

wait_until_n_fullsyncs(Node, N) ->
    %Status0 = rpc:call(Node, riak_repl_console, status, [quiet]),
    %Count = proplists:get_value(server_fullsyncs, Status0) + N,
    lager:info("Waiting for fullsync count to be ~p", [N]),

    Res = rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(server_fullsyncs, Status) of
                    C when C >= N ->
                        true;
                    Other ->
                        lager:info("Total fullsyncs = ~p", [Other]),
                        false
                end
        end),
    ?assertEqual(ok, Res),

    lager:info("Fullsync on ~p complete", [Node]).

