-module(rt_basic_test).
-export([rt_basic_test/0]).

rt_basic_test() ->
    lager:info("Deploy some nodes"),
    Nodes = rt:deploy_nodes(2),
    lager:info("Stop the nodes"),
    [rt:stop(Node) || Node <- Nodes],
    ok.
