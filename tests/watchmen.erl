-module(watchmen).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    pass.

always_pass() -> pass.
always_fail() -> ?assert(fail).
randomly() ->
    Nodes = rt:deploy_nodes(2),
    lager:info("Stop the nodes"),
    [rt:stop(Node) || Node <- Nodes],
    random:seed(now()),
    Rando = random:uniform(100),
    case  {Rando, Rando > 50} of
        {Rando, true} -> 
            ?assertEqual(-1, Rando);
        _ -> 
            pass
    end.
    