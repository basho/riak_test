-module(replication).
-compile(export_all).
-include("rt.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

replication() ->
    %% TODO: Don't hardcode # of nodes
    NumNodes = 6,
    ClusterASize = list_to_integer(get_os_env("CLUSTER_A_SIZE", "3")),
    %% ClusterBSize = NumNodes - ClusterASize,
    %% ClusterBSize = list_to_integer(get_os_env("CLUSTER_B_SIZE"), "2"),

    %% Nodes = rt:nodes(NumNodes),
    %% lager:info("Create dirs"),
    %% create_dirs(Nodes),

    Backend = list_to_atom(get_os_env("RIAK_BACKEND",
            "riak_kv_bitcask_backend")),

    lager:info("Deploy ~p nodes using ~p backend", [NumNodes, Backend]),
    Conf = [
            {riak_kv,
             [
                {storage_backend, Backend}
             ]},
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    Nodes = deploy_nodes(NumNodes, Conf),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    [AFirst|ARest] = ANodes,
    [join(ANode, AFirst) || ANode <- ARest],
    ?assertEqual(ok, wait_until_nodes_ready(ANodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(ANodes)),

    %% write some initial data to A
    lager:info("Writing 100 keys to ~p", [AFirst]),
    rt:systest_write(AFirst, 1, 100, <<"systest_a">>, 2),

    lager:info("Build cluster B"),
    [BFirst|BRest] = BNodes,
    [join(BNode, BFirst) || BNode <- BRest],
    ?assertEqual(ok, wait_until_nodes_ready(BNodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(BNodes)),

    %% write some initial data to B
    lager:info("Writing 100 keys to ~p", [BFirst]),
    rt:systest_write(BFirst, 1, 100, <<"systest_b">>, 2),

    %% setup servers/listeners on A
    Listeners = add_listeners(ANodes),

    %% verify servers are visible on all nodes
    verify_listeners(Listeners),

    %% get the leader for the first cluster
    LeaderA = rpc:call(AFirst, riak_repl_leader, leader_node, []),

    %% list of listeners not on the leader node
    NonLeaderListeners = lists:keydelete(LeaderA, 3, Listeners),

    %% setup sites on B
    %% TODO: make `NumSites' an argument
    NumSites = 4,
    {Ip, Port, _} = hd(NonLeaderListeners),
    add_site(hd(BNodes), {Ip, Port, "site1"}),
    FakeListeners = gen_fake_listeners(NumSites-1),
    add_fake_sites(BNodes, FakeListeners),

    %% verify sites are distributed on B
    verify_sites_balanced(NumSites, BNodes),

    %% check the listener IPs were all imported into the site
    verify_site_ips(BFirst, "site1", Listeners),

    %% get the leader for the first cluster
    LeaderB = rpc:call(BFirst, riak_repl_leader, leader_node, []),

    %% write some data on A
    lager:info("Writing 100 more keys to ~p", [AFirst]),
    rt:systest_write(AFirst, 101, 200, <<"systest_a">>, 2),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [AFirst, BFirst]),
    Res1 = rt:systest_read(BFirst, 101, 200, <<"systest_a">>, 2),
    ?assertEqual([], Res1),

    %% check that the keys we wrote initially aren't replicated yet, because
    %% we've disabled fullsync_on_connect
    lager:info("Check keys written before repl was connected are not present"),
    Res2 = rt:systest_read(BFirst, 1, 100, <<"systest_a">>, 2),
    ?assertEqual(100, length(Res2)),

    lager:info("Starting fullsync on ~p", [LeaderA]),
    rpc:call(LeaderA, riak_repl_console, start_fullsync, [[]]),
    wait_until_fullsync_complete(LeaderA, 1),
    lager:info("fullsync complete"),

    lager:info("Check keys written before repl was connected are present"),
    Res3 = rt:systest_read(BFirst, 1, 200, <<"systest_a">>, 2),
    ?assertEqual([], Res3),

    %%
    %% Failover tests
    %%

    lager:info("Testing master failover: stopping ~p", [LeaderA]),
    rt:stop(LeaderA),
    rt:wait_until_unpingable(LeaderA),
    ASecond = hd(ANodes -- [LeaderA]),
    wait_until_leader(ASecond),

    LeaderA2 = rpc:call(ASecond, riak_repl_leader, leader_node, []),

    lager:info("New leader is ~p", [LeaderA2]),

    wait_until_connection(LeaderA2),

    lager:info("Writing 100 more keys to ~p now that the old leader is down",
        [ASecond]),

    rt:systest_write(ASecond, 201, 300, <<"systest_a">>, 2),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [ASecond, BFirst]),
    Res4 = rt:systest_read(BFirst, 201, 300, <<"systest_a">>, 2),
    ?assertEqual([], Res4),

    lager:info("Testing client failover: stopping ~p", [LeaderB]),
    rt:stop(LeaderB),
    rt:wait_until_unpingable(LeaderB),
    BSecond = hd(BNodes -- [LeaderB]),
    wait_until_leader(BSecond),

    LeaderB2 = rpc:call(BSecond, riak_repl_leader, leader_node, []),

    lager:info("New leader is ~p", [LeaderB2]),

    wait_until_connection(LeaderA2),

    lager:info("Writing 100 more keys to ~p now that the old leader is down",
        [ASecond]),

    rt:systest_write(ASecond, 301, 400, <<"systest_a">>, 2),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [ASecond, BSecond]),
    Res5 = rt:systest_read(BSecond, 301, 400, <<"systest_a">>, 2),
    ?assertEqual([], Res5),

    %% Testing fullsync with downed nodes
    lager:info("Re-running fullsync with ~p and ~p down", [LeaderA, LeaderB]),

    lager:info("Starting fullsync on ~p", [LeaderA2]),
    rpc:call(LeaderA2, riak_repl_console, start_fullsync, [[]]),
    wait_until_fullsync_complete(LeaderA2, 1),
    lager:info("fullsync complete"),

    %%
    %% Per-bucket repl settings tests
    %%

    lager:info("Restarting down nodes ~p and ~p", [LeaderA, LeaderB]),
    rt:start(LeaderA),
    rt:start(LeaderB),
    rt:wait_until_pingable(LeaderA),
    rt:wait_until_pingable(LeaderB),

    lager:info("Nodes restarted"),

    make_bucket(LeaderA, <<"fullsync_only">>, [{repl, fullsync}]),
    make_bucket(LeaderA, <<"realtime_only">>, [{repl, realtime}]),
    make_bucket(LeaderA, <<"no_repl">>, [{repl, false}]),

    %% disconnect the other cluster, so realtime doesn't happen
    lager:info("disconnect the 2 clusters"),
    del_site(LeaderB, "site1"),

    lager:info("write 100 keys to a realtime only bucket"),
    rt:systest_write(ASecond, 1, 100, <<"realtime_only">>, 2),

    lager:info("reconnect the 2 clusters"),
    add_site(LeaderB, {Ip, Port, "site1"}),
    wait_until_connection(LeaderA),

    LeaderA3 = rpc:call(ASecond, riak_repl_leader, leader_node, []),

    lager:info("write 100 keys to a fullsync only bucket"),
    rt:systest_write(ASecond, 1, 100, <<"fullsync_only">>, 2),
    lager:info("write 100 keys to a {repl, false} bucket"),
    rt:systest_write(ASecond, 1, 100, <<"no_repl">>, 2),

    lager:info("Check the fullsync only bucket didn't replicate the writes"),
    Res6 = rt:systest_read(BSecond, 1, 100, <<"fullsync_only">>, 2),
    ?assertEqual(100, length(Res6)),

    lager:info("Check the realtime only bucket that was written to offline "
        "isn't replicated"),
    Res7 = rt:systest_read(BSecond, 1, 100, <<"realtime_only">>, 2),
    ?assertEqual(100, length(Res7)),

    lager:info("Check the {repl, false} bucket didn't replicate"),
    Res8 = rt:systest_read(BSecond, 1, 100, <<"no_repl">>, 2),
    ?assertEqual(100, length(Res8)),

    %% do a fullsync, make sure that fullsync_only is replicated, but
    %% realtime_only and no_repl aren't
    lager:info("Starting fullsync on ~p", [LeaderA3]),
    rpc:call(LeaderA3, riak_repl_console, start_fullsync, [[]]),
    wait_until_fullsync_complete(LeaderA3, 1),
    lager:info("fullsync complete"),

    lager:info("Check fullsync only bucket is now replicated"),
    Res9 = rt:systest_read(BSecond, 1, 100, <<"fullsync_only">>, 2),
    ?assertEqual([], Res9),

    lager:info("Check realtime only bucket didn't replicate"),
    Res10 = rt:systest_read(BSecond, 1, 100, <<"realtime_only">>, 2),
    ?assertEqual(100, length(Res10)),

    lager:info("Check {repl, false} bucket didn't replicate"),
    Res10 = rt:systest_read(BSecond, 1, 100, <<"no_repl">>, 2),
    ?assertEqual(100, length(Res10)),

    lager:info("Write 100 more keys into realtime only bucket"),
    rt:systest_write(ASecond, 101, 200, <<"realtime_only">>, 2),

    timer:sleep(5000),

    lager:info("Check the realtime keys replicated"),
    Res11 = rt:systest_read(BSecond, 101, 200, <<"realtime_only">>, 2),
    ?assertEqual([], Res11),

    lager:info("Check the older keys in the realtime bucket did not replicate"),
    Res12 = rt:systest_read(BSecond, 1, 100, <<"realtime_only">>, 2),
    ?assertEqual(100, length(Res12)),

    lager:info("Test passed"),
    fin.

verify_sites_balanced(NumSites, BNodes0) ->
    Leader = rpc:call(hd(BNodes0), riak_repl_leader, leader_node, []),
    BNodes = BNodes0 -- [Leader],
    NumNodes = length(BNodes),
    NodeCounts = [{Node, client_count(Node)} || Node <- BNodes],
    lager:notice("nodecounts ~p", [NodeCounts]),
    lager:notice("leader ~p", [Leader]),
    Min = NumSites div NumNodes,
    [?assert(Count >= Min) || {_Node, Count} <- NodeCounts].

client_count(Node) ->
    Clients = rpc:call(Node, supervisor, which_children, [riak_repl_client_sup]),
    length(Clients).

gen_fake_listeners(Num) ->
    Ports = gen_ports(11000, Num),
    IPs = lists:duplicate(Num, "127.0.0.1"),
    Nodes = [fake_node(N) || N <- lists:seq(1, Num)],
    lists:zip3(IPs, Ports, Nodes).

fake_node(Num) ->
    lists:flatten(io_lib:format("fake~p@127.0.0.1", [Num])).

add_fake_sites([Node|_], Listeners) ->
    [add_site(Node, {IP, Port, fake_site(Port)})
     || {IP, Port, _} <- Listeners].

add_site(Node, {IP, Port, Name}) ->
    lager:info("Add site ~p ~p:~p at node ~p", [Name, IP, Port, Node]),
    Args = [IP, integer_to_list(Port), Name],
    Res = rpc:call(Node, riak_repl_console, add_site, [Args]),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(5)).

del_site(Node, Name) ->
    lager:info("Del site ~p at ~p", [Name, Node]),
    Res = rpc:call(Node, riak_repl_console, del_site, [[Name]]),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(5)).

fake_site(Port) ->
    lists:flatten(io_lib:format("fake_site_~p", [Port])).

verify_listeners(Listeners) ->
    Strs = [IP ++ ":" ++ integer_to_list(Port) || {IP, Port, _} <- Listeners],
    [verify_listener(Node, Strs) || {_, _, Node} <- Listeners].

verify_listener(Node, Strs) ->
    lager:info("Verify listeners ~p ~p", [Node, Strs]),
    Status = rpc:call(Node, riak_repl_console, status, [quiet]),
    [verify_listener(Node, Str, Status) || Str <- Strs].

verify_listener(Node, Str, Status) ->
    lager:info("Verify listener ~s is seen by node ~p", [Str, Node]),
    ?assert(lists:keymember(Str, 2, Status)).

add_listeners(Nodes) ->
    Ports = gen_ports(9010, length(Nodes)),
    IPs = lists:duplicate(length(Nodes), "127.0.0.1"),
    PN = lists:zip3(IPs, Ports, Nodes),
    [add_listener(Node, IP, Port) || {IP, Port, Node} <- PN],
    PN.

add_listener(Node, IP, Port) ->
    lager:info("Adding repl listener to ~p ~s:~p", [Node, IP, Port]),
    Args = [[atom_to_list(Node), IP, integer_to_list(Port)]],
    Res = rpc:call(Node, riak_repl_console, add_listener, Args),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(3)).

gen_ports(Start, Len) ->
    lists:seq(Start, Start + Len - 1).

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

verify_site_ips(Leader, Site, Listeners) ->
    Status = rpc:call(Leader, riak_repl_console, status, [quiet]),
    Key = lists:flatten([Site, "_ips"]),
    IPStr = proplists:get_value(Key, Status),
    IPs = lists:sort(re:split(IPStr, ", ")),
    ExpectedIPs = lists:sort(
        [list_to_binary([IP, ":", integer_to_list(Port)]) || {IP, Port, _Node} <-
            Listeners]),
    ?assertEqual(ExpectedIPs, IPs).

make_bucket(Node, Name, Args) ->
    Res = rpc:call(Node, riak_core_bucket, set_bucket, [Name, Args]),
    ?assertEqual(ok, Res).

wait_until_fullsync_complete(Node, Count) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(server_fullsyncs, Status) of
                    Count ->
                        true;
                    _ ->
                        false
                end
        end).

wait_until_leader(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(leader, Status) of
                    undefined ->
                        false;
                    _ ->
                        true
                end
        end).

wait_until_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(server_stats, Status) of
                    [] ->
                        false;
                    _ ->
                        true
                end
        end).
