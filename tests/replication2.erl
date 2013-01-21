-module(replication2).
-behavior(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

confirm() ->
    %% TODO: Don't hardcode # of nodes
    NumNodes = 6,
    ClusterASize = list_to_integer(get_os_env("CLUSTER_A_SIZE", "3")),
    %% ClusterBSize = NumNodes - ClusterASize,
    %% ClusterBSize = list_to_integer(get_os_env("CLUSTER_B_SIZE"), "2"),

    %% Nodes = rt:nodes(NumNodes),
    %% lager:info("Create dirs"),
    %% create_dirs(Nodes),

    %% Backends now configured via cli or giddyup
    %%Backend = list_to_atom(get_os_env("RIAK_BACKEND",
    %%        "riak_kv_bitcask_backend")),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_kv,
                [
                    {anti_entropy, {off, []}}
                ]
            },
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    Nodes = deploy_nodes(NumNodes, Conf),
    %%Nodes = rt:deploy_nodes([
    %%    {"1.2.0", Conf},
    %%    {"1.2.0", Conf},
    %%    {"1.2.0", Conf},
    %%    {"1.2.0", Conf},
    %%    {"1.2.0", Conf},
    %%    {"1.2.0", Conf}
    %%    ]),


    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    make_cluster(ANodes),

    lager:info("Build cluster B"),
    make_cluster(BNodes),

    replication(ANodes, BNodes, false),
    pass.

replication([AFirst|_] = ANodes, [BFirst|_] = BNodes, Connected) ->

    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,
    FullsyncOnly = <<TestHash/binary, "-fullsync_only">>,
    RealtimeOnly = <<TestHash/binary, "-realtime_only">>,
    NoRepl = <<TestHash/binary, "-no_repl">>,

    case Connected of
        false ->
            %% clusters are not connected, connect them

            %% write some initial data to A
            lager:info("Writing 100 keys to ~p", [AFirst]),
            ?assertEqual([], do_write(AFirst, 1, 100, TestBucket, 2)),

            name_cluster(AFirst, "A"),
            name_cluster(BFirst, "B"),
            rt:wait_until_ring_converged(ANodes),
            rt:wait_until_ring_converged(BNodes),

            %% TODO: we'll need to wait for cluster names before continuing

            %% get the leader for the first cluster
            wait_until_leader(AFirst),
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

            {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                [riak_core, cluster_mgr]),
            connect_cluster(LeaderA, "127.0.0.1", Port),

            ?assertEqual(ok, wait_for_connection(LeaderA, "B")),
            enable_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            start_realtime(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes),
            enable_fullsync(LeaderA, "B"),
            rt:wait_until_ring_converged(ANodes);
        _ ->
            lager:info("waiting for leader to converge on cluster A"),
            ?assertEqual(ok, wait_until_leader_converge(ANodes)),
            lager:info("waiting for leader to converge on cluster B"),
            ?assertEqual(ok, wait_until_leader_converge(BNodes)),
            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
            lager:info("Leader on cluster A is ~p", [LeaderA]),
            {ok, {_IP, Port}} = rpc:call(BFirst, application, get_env,
                [riak_core, cluster_mgr])
    end,

    %% write some data on A
    ?assertEqual(ok, wait_for_connection(LeaderA, "B")),
    %io:format("~p~n", [rpc:call(LeaderA, riak_repl_console, status, [quiet])]),
    lager:info("Writing 100 more keys to ~p", [LeaderA]),
    ?assertEqual([], do_write(LeaderA, 101, 200, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, wait_for_reads(BFirst, 101, 200, TestBucket, 2)),

    case Connected of
        false ->
            %% check that the keys we wrote initially aren't replicated yet, because
            %% we've disabled fullsync_on_connect
            lager:info("Check keys written before repl was connected are not present"),
            Res2 = rt:systest_read(BFirst, 1, 100, TestBucket, 2),
            ?assertEqual(100, length(Res2)),

            start_and_wait_until_fullsync_complete(LeaderA),

            lager:info("Check keys written before repl was connected are present"),
            ?assertEqual(0, wait_for_reads(BFirst, 1, 200, TestBucket, 2));
        _ ->
            ok
    end,

    %%
    %% Failover tests
    %%

    lager:info("Testing master failover: stopping ~p", [LeaderA]),
    rt:stop(LeaderA),
    rt:wait_until_unpingable(LeaderA),
    ASecond = hd(ANodes -- [LeaderA]),
    wait_until_leader(ASecond),

    LeaderA2 = rpc:call(ASecond, riak_core_cluster_mgr, get_leader, []),

    lager:info("New leader is ~p", [LeaderA2]),

    ?assertEqual(ok, wait_until_connection(LeaderA2)),

    lager:info("Writing 100 more keys to ~p now that the old leader is down",
        [ASecond]),

    ?assertEqual([], do_write(ASecond, 201, 300, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [ASecond, BFirst]),
    ?assertEqual(0, wait_for_reads(BFirst, 201, 300, TestBucket, 2)),

    %% get the leader for the first cluster
    LeaderB = rpc:call(BFirst, riak_core_cluster_mgr, get_leader, []),

    lager:info("Testing client failover: stopping ~p", [LeaderB]),
    rt:stop(LeaderB),
    rt:wait_until_unpingable(LeaderB),
    BSecond = hd(BNodes -- [LeaderB]),
    wait_until_leader(BSecond),

    LeaderB2 = rpc:call(BSecond, riak_core_cluster_mgr, get_leader, []),

    lager:info("New leader is ~p", [LeaderB2]),

    ?assertEqual(ok, wait_until_connection(LeaderA2)),

    lager:info("Writing 100 more keys to ~p now that the old leader is down",
        [ASecond]),

    ?assertEqual([], do_write(ASecond, 301, 400, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 101 keys written to ~p from ~p", [ASecond, BSecond]),
    ?assertEqual(0, wait_for_reads(BSecond, 301, 400, TestBucket, 2)),

    %% Testing fullsync with downed nodes
    lager:info("Re-running fullsync with ~p and ~p down", [LeaderA, LeaderB]),

    start_and_wait_until_fullsync_complete(LeaderA2),

    %%
    %% Per-bucket repl settings tests
    %%

    lager:info("Restarting down node ~p", [LeaderA]),
    rt:start(LeaderA),
    rt:wait_until_pingable(LeaderA),
    start_and_wait_until_fullsync_complete(LeaderA2),


    lager:info("Starting Joe's Repl Test"),

    %% @todo add stuff
    %% At this point, realtime sync should still work, but, it doesn't because of a bug in 1.2.1 
    %% Check that repl leader is LeaderA
    %% Check that LeaderA2 has ceeded socket back to LeaderA

    lager:info("Leader: ~p", [rpc:call(ASecond, riak_core_cluster_mgr, get_leader, [])]),
    lager:info("LeaderA: ~p", [LeaderA]),
    lager:info("LeaderA2: ~p", [LeaderA2]),

    ?assertEqual(ok, wait_until_connection(LeaderA)),

    lager:info("Simulation partition to force leader re-election"),

    OldCookie = rpc:call(LeaderA2, erlang, get_cookie, []),
    NewCookie = list_to_atom(lists:reverse(atom_to_list(OldCookie))),
    rpc:call(LeaderA2, erlang, set_cookie, [LeaderA2, NewCookie]),

    [ rpc:call(LeaderA2, erlang, disconnect_node, [Node]) || Node <- ANodes -- [LeaderA2]],
    [ rpc:call(Node, erlang, disconnect_node, [LeaderA2]) || Node <- ANodes -- [LeaderA2]],

    wait_until_new_leader(hd(ANodes -- [LeaderA2]), LeaderA2),
    InterimLeader = rpc:call(LeaderA, riak_core_cluster_mgr, get_leader, []),
    lager:info("Interim leader: ~p", [InterimLeader]),

    %rpc:call(LeaderA2, erlang, apply, [fun() -> [net_adm:ping(N) || N <- ANodes] end, []]),
    rpc:call(LeaderA2, erlang, set_cookie, [LeaderA2, OldCookie]),

    [ rpc:call(LeaderA2, net_adm, ping, [Node]) || Node <- ANodes -- [LeaderA2]],
    [ rpc:call(Node, net_adm, ping, [LeaderA2]) || Node <- ANodes -- [LeaderA2]],

    %% there's no point in writing anything until the leaders converge, as we
    %% can drop writes in the middle of an election
    wait_until_leader_converge(ANodes),

    lager:info("Leader: ~p", [rpc:call(ASecond, riak_core_cluster_mgr, get_leader, [])]),
    lager:info("Writing 2 more keys to ~p", [LeaderA]),
    ?assertEqual([], do_write(LeaderA, 1301, 1302, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 2 keys written to ~p from ~p", [LeaderA, BSecond]),
    ?assertEqual(0, wait_for_reads(BSecond, 1301, 1302, TestBucket, 2)),

    lager:info("Finished Joe's Section"),

    lager:info("Restarting down node ~p", [LeaderB]),
    rt:start(LeaderB),
    rt:wait_until_pingable(LeaderB),

    lager:info("Nodes restarted"),

    make_bucket(LeaderA, NoRepl, [{repl, false}]),

    make_bucket(LeaderA, RealtimeOnly, [{repl, realtime}]),
    make_bucket(LeaderA, FullsyncOnly, [{repl, fullsync}]),

    %% disconnect the other cluster, so realtime doesn't happen
    lager:info("disconnect the 2 clusters"),
    disable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    disconnect_cluster(LeaderA, "B"),
    wait_until_no_connection(LeaderA),
    rt:wait_until_ring_converged(ANodes),

    lager:info("write 100 keys to a realtime only bucket"),
    ?assertEqual([], do_write(ASecond, 1, 100,
            RealtimeOnly, 2)),

    lager:info("reconnect the 2 clusters"),
    connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),
    enable_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    start_realtime(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),
    ?assertEqual(ok, wait_until_connection(LeaderA)),

    LeaderA3 = rpc:call(ASecond, riak_core_cluster_mgr, get_leader, []),

    lager:info("write 100 keys to a {repl, false} bucket"),
    ?assertEqual([], do_write(ASecond, 1, 100, NoRepl, 2)),

    lager:info("write 100 keys to a fullsync only bucket"),
    ?assertEqual([], do_write(ASecond, 1, 100,
            FullsyncOnly, 2)),

    lager:info("Check the fullsync only bucket didn't replicate the writes"),
    Res6 = rt:systest_read(BSecond, 1, 100, FullsyncOnly, 2),
    ?assertEqual(100, length(Res6)),

    lager:info("Check the realtime only bucket that was written to offline "
        "isn't replicated"),
    Res7 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res7)),

    lager:info("Check the {repl, false} bucket didn't replicate"),
    Res8 = rt:systest_read(BSecond, 1, 100, NoRepl, 2),
    ?assertEqual(100, length(Res8)),

    %% do a fullsync, make sure that fullsync_only is replicated, but
    %% realtime_only and no_repl aren't
    start_and_wait_until_fullsync_complete(LeaderA3),

    lager:info("Check fullsync only bucket is now replicated"),
    ?assertEqual(0, wait_for_reads(BSecond, 1, 100,
            FullsyncOnly, 2)),

    lager:info("Check realtime only bucket didn't replicate"),
    Res10 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res10)),


    lager:info("Write 100 more keys into realtime only bucket on ~p",
        [ASecond]),
    ?assertEqual([], do_write(ASecond, 101, 200,
            RealtimeOnly, 2)),

    timer:sleep(5000),

    lager:info("Check the realtime keys replicated"),
    ?assertEqual(0, wait_for_reads(BSecond, 101, 200,
            RealtimeOnly, 2)),

    lager:info("Check the older keys in the realtime bucket did not replicate"),
    Res12 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
    ?assertEqual(100, length(Res12)),

    lager:info("Check {repl, false} bucket didn't replicate"),
    Res13 = rt:systest_read(BSecond, 1, 100, NoRepl, 2),
    ?assertEqual(100, length(Res13)),

    lager:info("Testing offline realtime queueing"),

    LeaderA4 = rpc:call(ASecond, riak_core_cluster_mgr, get_leader, []),

    lager:info("Stopping realtime, queue will build"),
    stop_realtime(LeaderA4, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Writing 100 keys"),
    ?assertEqual([], do_write(LeaderA4, 800, 900,
            TestBucket, 2)),

    lager:info("Starting realtime"),
    start_realtime(LeaderA4, "B"),
    rt:wait_until_ring_converged(ANodes),
    timer:sleep(3000),

    lager:info("Reading keys written while repl was stopped"),
    ?assertEqual(0, wait_for_reads(BSecond, 800, 900,
            TestBucket, 2)),

    lager:info("Testing realtime migration on node shutdown"),
    Target = hd(ANodes -- [LeaderA4]),

    lager:info("Stopping realtime, queue will build"),
    stop_realtime(LeaderA4, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Writing 100 keys"),
    ?assertEqual([], do_write(Target, 900, 1000,
            TestBucket, 2)),

    io:format("queue status: ~p",
              [rpc:call(Target, riak_repl2_rtq, status, [])]),

    lager:info("Stopping node ~p", [Target]),

    rt:stop(Target),
    rt:wait_until_unpingable(Target),

    lager:info("Starting realtime"),
    start_realtime(LeaderA4, "B"),
    timer:sleep(3000),

    lager:info("Reading keys written while repl was stopped"),
    ?assertEqual(0, wait_for_reads(BSecond, 900, 1000,
            TestBucket, 2)),

    lager:info("Restarting node ~p", [Target]),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    timer:sleep(5000),

    pb_write_during_shutdown(Target, BSecond, TestBucket),
    timer:sleep(5000),
    http_write_during_shutdown(Target, BSecond, TestBucket),

    lager:info("Test passed"),
    fin.

pb_write_during_shutdown(Target, BSecond, TestBucket) ->
    {ok, Port} = rpc:call(Target, application, get_env, [riak_api, pb_port]),

    lager:info("Connecting to pb socket ~p:~p on ~p", ["127.0.0.1", Port,
            Target]),
    {ok, PBSock} = riakc_pb_socket:start("127.0.0.1", Port),

    %% do the stop in the background while we're writing keys
    spawn(fun() ->
                timer:sleep(500),
                lager:info("Stopping node ~p again", [Target]),
                rt:stop(Target),
                lager:info("Node stopped")
           end),

    lager:info("Writing 10,000 keys"),
    WriteErrors =
        try
          pb_write(PBSock, 1000, 11000, TestBucket, 2)
        catch
          _:_ ->
            lager:info("Shutdown timeout caught"),
            []
        end,
    lager:info("got ~p write failures", [length(WriteErrors)]),
    timer:sleep(3000),
    lager:info("checking number of read failures on secondary cluster"),
    ReadErrors = rt:systest_read(BSecond, 1000, 11000, TestBucket, 2),
    lager:info("got ~p read failures", [length(ReadErrors)]),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    timer:sleep(5000),
    ReadErrors2 = rt:systest_read(Target, 1000, 11000, TestBucket, 2),
    lager:info("got ~p read failures on ~p", [length(ReadErrors2), Target]),
    case length(WriteErrors) >= length(ReadErrors) of
        true ->
            ok;
        false ->
            lager:error("Got more read errors on ~p: ~p than write "
                "errors on ~p: ~p",
                        [BSecond, length(ReadErrors), Target,
                            length(WriteErrors)]),
            FailedKeys = lists:foldl(fun({Key, _}, Acc) ->
                        case lists:keyfind(Key, 1, WriteErrors) of
                            false ->
                                [Key|Acc];
                            _ ->
                                Acc
                        end
                end, [], ReadErrors),
            lager:info("failed keys ~p", [FailedKeys]),
            ?assert(false)
    end.

http_write_during_shutdown(Target, BSecond, TestBucket) ->
    {ok, [{_IP, Port}|_]} = rpc:call(Target, application, get_env, [riak_core, http]),

    lager:info("Connecting to http socket ~p:~p on ~p", ["127.0.0.1", Port,
            Target]),
    C = rhc:create("127.0.0.1", Port, "riak", []),

    %% do the stop in the background while we're writing keys
    spawn(fun() ->
                timer:sleep(500),
                lager:info("Stopping node ~p again", [Target]),
                rt:stop(Target),
                lager:info("Node stopped")
           end),

    lager:info("Writing 10,000 keys"),
    WriteErrors =
        try
          http_write(C, 12000, 22000, TestBucket, 2)
        catch
          _:_ ->
            lager:info("Shutdown timeout caught"),
            []
        end,
    lager:info("got ~p write failures", [length(WriteErrors)]),
    timer:sleep(3000),
    lager:info("checking number of read failures on secondary cluster"),
    {ok, [{_IP, Port2}|_]} = rpc:call(BSecond, application, get_env, [riak_core, http]),
    C2 = rhc:create("127.0.0.1", Port2, "riak", []),
    ReadErrors = http_read(C2, 12000, 22000, TestBucket, 2),
    lager:info("got ~p read failures", [length(ReadErrors)]),

    rt:start(Target),
    rt:wait_until_pingable(Target),
    timer:sleep(5000),
    ReadErrors2 = http_read(C, 12000, 22000, TestBucket, 2),
    lager:info("got ~p read failures on ~p", [length(ReadErrors2), Target]),
    case length(WriteErrors) >= length(ReadErrors) of
        true ->
            ok;
        false ->
            lager:error("Got more read errors on ~p: ~p than write "
                "errors on ~p: ~p",
                        [BSecond, length(ReadErrors), Target,
                            length(WriteErrors)]),
            FailedKeys = lists:foldl(fun({Key, _}, Acc) ->
                        case lists:keyfind(Key, 1, WriteErrors) of
                            false ->
                                [Key|Acc];
                            _ ->
                                Acc
                        end
                end, [], ReadErrors),
            lager:info("failed keys ~p", [FailedKeys]),
            ?assert(false)
    end.

client_iterate(_Sock, [], _Bucket, _W, Acc, _Fun, Parent) ->
    Parent ! {result, self(), Acc},
    Acc;

client_iterate(Sock, [N | NS], Bucket, W, Acc, Fun, Parent) ->
    NewAcc = try Fun(Sock, Bucket, N, W) of
        ok ->
            Acc;
        Other ->
            [{N, Other} | Acc]
    catch
        What:Why ->
            [{N, {What, Why}} | Acc]
    end,
    client_iterate(Sock, NS, Bucket, W, NewAcc, Fun, Parent).

http_write(Sock, Start, End, Bucket, W) ->
    F = fun(S, B, K, WVal) ->
            X = list_to_binary(integer_to_list(K)),
            Obj = riakc_obj:new(B, X, X),
            rhc:put(S, Obj, [{w, WVal}])
    end,
    Keys = lists:seq(Start, End),
    Partitions = partition_keys(Keys, 8),
    Parent = self(),
    Workers = [spawn_monitor(fun() -> client_iterate(Sock, K, Bucket, W, [], F,
                    Parent) end) || K <- Partitions],
    collect_results(Workers, []).

pb_write(Sock, Start, End, Bucket, W) ->
    %pb_iterate(Sock, lists:seq(Start, End), Bucket, W, []).
    F = fun(S, B, K, WVal) ->
            Obj = riakc_obj:new(B, <<K:32/integer>>, <<K:32/integer>>),
            riakc_pb_socket:put(S, Obj, [{w, WVal}])
    end,
    Keys = lists:seq(Start, End),
    Partitions = partition_keys(Keys, 8),
    Parent = self(),
    Workers = [spawn_monitor(fun() -> client_iterate(Sock, K, Bucket, W, [], F,
                    Parent) end) || K <- Partitions],
    collect_results(Workers, []).

http_read(Sock, Start, End, Bucket, R) ->
    F = fun(S, B, K, RVal) ->
            X = list_to_binary(integer_to_list(K)),
            case rhc:get(S, B, X, [{r, RVal}]) of
                {ok, _} ->
                    ok;
                Error ->
                    Error
            end
    end,
    client_iterate(Sock, lists:seq(Start, End), Bucket, R, [], F, self()).

partition_keys(Keys, PC) ->
    partition_keys(Keys, PC, lists:duplicate(PC, [])).

partition_keys([] , _, Acc) ->
    Acc;
partition_keys(Keys, PC, Acc) ->
    In = lists:sublist(Keys, PC),
    Rest = try lists:nthtail(PC, Keys)
        catch _:_ -> []
    end,
    NewAcc = lists:foldl(fun(K, [H|T]) ->
                T ++ [[K|H]]
        end, Acc, In),
    partition_keys(Rest, PC, NewAcc).

collect_results([], Acc) ->
    Acc;
collect_results(Workers, Acc) ->
    receive
        {result, Pid, Res} ->
            collect_results(lists:keydelete(Pid, 1, Workers), Res ++ Acc);
        {'DOWN', _, _, Pid, _Reason} ->
            collect_results(lists:keydelete(Pid, 1, Workers), Acc)
    end.

make_cluster(Nodes) ->
    [First|Rest] = Nodes,
    [join(Node, First) || Node <- Rest],
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)).

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

client_count(Node) ->
    Clients = rpc:call(Node, supervisor, which_children, [riak_repl_client_sup]),
    length(Clients).

get_os_env(Var) ->
    case get_os_env(Var, undefined) of
        undefined -> exit({os_env_var_undefined, Var});
        Value -> Value
    end.

get_os_env(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> Value
    end.

make_bucket(Node, Name, Args) ->
    Res = rpc:call(Node, riak_core_bucket, set_bucket, [Name, Args]),
    ?assertEqual(ok, Res).

start_and_wait_until_fullsync_complete(Node) ->
    Status0 = rpc:call(Node, riak_repl_console, status, [quiet]),
    Count = proplists:get_value(server_fullsyncs, Status0) + 1,
    lager:info("waiting for fullsync count to be ~p", [Count]),

    lager:info("Starting fullsync on ~p (~p)", [Node,
            rtdev:node_version(rtdev:node_id(Node))]),
    rpc:call(Node, riak_repl_console, fullsync, [["start"]]),
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

wait_until_is_leader(Node) ->
    lager:info("wait_until_is_leader(~p)", [Node]),
    rt:wait_until(Node, fun is_leader/1).

is_leader(Node) ->
    case rpc:call(Node, riak_core_cluster_mgr, get_leader, []) of
        {badrpc, _} ->
            lager:info("Badrpc"),
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
        {badrpc, _} ->
            lager:info("Badrpc"),
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
                length(lists:usort([begin
                        case rpc:call(N, riak_core_cluster_mgr, get_leader, []) of
                            undefined ->
                                false;
                            L ->
                                %lager:info("Leader for ~p is ~p",
                                %[N,L]),
                                L
                        end
                end || N <- Nodes])) == 1
        end).

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
                rt:systest_read(Node, Start, End, Bucket, R) == []
        end),
    Reads = rt:systest_read(Node, Start, End, Bucket, R),
    lager:info("Reads: ~p", [Reads]),
    length(Reads).

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

name_cluster(Node, Name) ->
    lager:info("Naming cluster ~p",[Name]),
    Res = rpc:call(Node, riak_repl_console, clustername, [[Name]]),
    ?assertEqual(ok, Res).

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
                {ok, Connections} = rpc:call(Node, riak_core_cluster_mgr,
                    get_connections, []),
                lists:any(fun({{cluster_by_name, N}, _}) when N == Name -> true;
                        (_) -> false
                    end, Connections)
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

start_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["start", Cluster]]),
    ?assertEqual(ok, Res).

stop_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["stop", Cluster]]),
    ?assertEqual(ok, Res).
