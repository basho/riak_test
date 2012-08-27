-module(rt_basic_test).
-export([confirm/0]).

confirm() ->
    lager:info("Deploy some nodes"),
    Nodes = rt:deploy_nodes(2),
    lager:info("Stop the nodes"),
    [rt:stop(Node) || Node <- Nodes],
    pass.
