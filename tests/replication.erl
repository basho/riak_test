-module(replication).
-compile(export_all).
-include("rt.hrl").

-import(rt, [deploy_nodes/1,
             join/2]).

replication() ->
    %% TODO: Don't hardcode # of nodes
    NumNodes = 6,
    ClusterASize = list_to_integer(get_os_env("CLUSTER_A_SIZE", "4")),
    %% ClusterBSize = NumNodes - ClusterASize,
    %% ClusterBSize = list_to_integer(get_os_env("CLUSTER_B_SIZE"), "2"),

    %% Nodes = rt:nodes(NumNodes),
    %% lager:info("Create dirs"),
    %% create_dirs(Nodes),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Nodes = deploy_nodes(NumNodes),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),


    lager:info("Build cluster A"),
    [AFirst|ARest] = ANodes,
    [join(ANode, AFirst) || ANode <- ARest],

    lager:info("Build cluster B"),
    [BFirst|BRest] = BNodes,
    [join(BNode, BFirst) || BNode <- BRest],

    %% setup servers/listeners on A
    Listeners = add_listeners(ANodes),

    %% verify servers are visible on all nodes
    verify_listeners(Listeners),

    %% setup sites on B
    %% TODO: make `NumSites' an argument
    {Ip, Port} = hd(Listeners),
    add_site(hd(BNodes), {Ip, Port, "site1"}),
    NumSites = 4,
    FakeListeners = gen_fake_listeners(NumSites-1),
    add_fake_sites(BNodes, [hd(Listeners)|FakeListeners]),

    %% verify sites are distributed on B

    %% write some data on A

    %% verify data is replicated to B

    fin.

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
    lager:info("Add site ~p ~p:~p", [Name, IP, Port]),
    Args = [IP, integer_to_list(Port), Name],
    Res = rpc:call(Node, riak_repl_console, add_site, [Args]),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(3)).

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
    %% Start = 9010,
    Ports = gen_ports(9010, length(Nodes)),%lists:seq(Start, Start + length(Nodes) - 1),
    IPs = lists:duplicate(length(Nodes), "127.0.0.1"),
    PN = lists:zip3(IPs, Ports, Nodes),
    [add_listener(Node, IP, Port) || {IP, Port, Node} <- PN],
    PN.

add_listener(Node, IP, Port) ->
    lager:info("Adding repl listener to ~p ~s:~p", [Node, IP, Port]),
    Args = [[atom_to_list(Node), IP, integer_to_list(Port)]],
    Res = rpc:call(Node, riak_repl_console, add_listener, Args),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(5)).

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
