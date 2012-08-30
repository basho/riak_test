-module(rolling_capabilities).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Count = 4,
    OldVsn = "1.1.4",
    %% Assuming default 1.1.4 app.config settings, the only difference
    %% between rolling and upgraded should be 'staged_joins'. Explicitly
    %% test rolling values to ensure we don't fallback to default settings.
    ExpectedOld = [{riak_core, vnode_routing, proxy},
                   {riak_core, staged_joins, false},
                   {riak_kv, legacy_keylisting, false},
                   {riak_kv, listkeys_backpressure, true},
                   {riak_kv, mapred_2i_pipe, true},
                   {riak_kv, mapred_system, pipe},
                   {riak_kv, vnode_vclocks, true}],
    ExpectedNew = [{riak_core, vnode_routing, proxy},
                   {riak_core, staged_joins, true},
                   {riak_kv, legacy_keylisting, false},
                   {riak_kv, listkeys_backpressure, true},
                   {riak_kv, mapred_2i_pipe, true},
                   {riak_kv, mapred_system, pipe},
                   {riak_kv, vnode_vclocks, true}],
    lager:info("Deploying Riak ~p cluster", [OldVsn]),
    Nodes = rt:build_cluster([OldVsn || _ <- lists:seq(1,Count)]),
    lists:foldl(fun(Node, Upgraded) ->
                        rt:upgrade(Node, current),
                        Upgraded2 = Upgraded ++ [Node],
                        lager:info("Verifying rolling/old capabilities"),
                        (Upgraded2 == Nodes)
                            orelse check_capabilities(Upgraded2, ExpectedOld),
                        Upgraded2
                end, [], Nodes),
    lager:info("Verifying final/upgraded capabilities"),
    check_capabilities(Nodes, ExpectedNew),
    lager:info("Test ~p passed", [?MODULE]),
    pass.

check_capabilities(Nodes, Expected) ->
    [?assertEqual(ok, rt:wait_until_capability(Node, {App, Cap}, Val))
     || {App, Cap, Val} <- Expected,
        Node <- Nodes],
    ok.
