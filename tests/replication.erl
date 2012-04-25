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

    %% verify servers are distributed on A
    verify_listeners(Listeners),

    %% setup sites on B

    %% verify sites are distributed on B

    %% write some data on A

    %% verify data is replicated to B

    fin.

verify_listeners(Listeners) ->
    Strs = ["127.0.0.1:" ++ integer_to_list(Port) || {Port, _} <- Listeners],
    [verify_listener(Node, Strs) || {_, Node} <- Listeners].

verify_listener(Node, Strs) ->
    lager:info("Verify listeners ~p ~p", [Node, Strs]),
    Status = rpc:call(Node, riak_repl_console, status, [quiet]),
    [verify_listener(Node, Str, Status) || Str <- Strs].

verify_listener(Node, Str, Status) ->
    lager:info("Verify listener ~s is seen by node ~p", [Str, Node]),
    ?assert(lists:keymember(Str, 2, Status)).

add_listeners(Nodes) ->
    Start = 9010,
    Ports = lists:seq(Start, Start + length(Nodes) - 1),
    PN = lists:zip(Ports, Nodes),
    [add_listener(Node, "127.0.0.1", Port) || {Port, Node} <- PN],
    PN.

add_listener(Node, IP, Port) ->
    lager:info("Adding repl listener to ~p ~s:~p", [Node, IP, Port]),
    Args = [[atom_to_list(Node), IP, integer_to_list(Port)]],
    Res = rpc:call(Node, riak_repl_console, add_listener, Args),
    ?assertEqual(ok, Res).

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
