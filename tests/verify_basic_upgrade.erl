-module(verify_basic_upgrade).
-export([verify_basic_upgrade/0]).
-include_lib("eunit/include/eunit.hrl").

verify_basic_upgrade() ->
    OldVsns = ["1.0.3", "1.1.4"],
    [build_cluster(OldVsn, current) || OldVsn <- OldVsns],
    [build_cluster(current, OldVsn) || OldVsn <- OldVsns],
    lager:info("Test ~p passed", [?MODULE]),
    ok.

build_cluster(Vsn1, Vsn2) ->
    lager:info("Testing versions: ~p <- ~p", [Vsn1, Vsn2]),
    Nodes = rt:deploy_nodes([Vsn1, Vsn2]),
    [Node1, Node2] = Nodes,
    lager:info("Writing 100 keys to ~p", [Node1]),
    timer:sleep(1000),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),

    lager:info("Join ~p to ~p", [Node2, Node1]),
    rt:join(Node2, Node1),

    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_ring_converged(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),

    (Vsn1 /= current) andalso rt:upgrade(Node1, current),
    (Vsn2 /= current) andalso rt:upgrade(Node2, current),

    timer:sleep(1000),
    lager:info("Ensuring keys still exist"),
    rt:systest_read(Node1, 100, 1),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),
    ok.
