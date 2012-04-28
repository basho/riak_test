-module(upgrade).
-export([upgrade/0]).
-include_lib("eunit/include/eunit.hrl").

upgrade() ->
    Nodes = rt:build_cluster(4, ["1.0.3", "1.0.3", "1.1.2", current]),
    [Node1, Node2, Node3, _Node4] = Nodes,

    lager:info("Writing 100 keys"),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),
    rtdev:upgrade(Node1, "1.1.2"),
    lager:info("Ensuring keys still exist"),
    rt:stop(Node2),
    rt:stop(Node3),
    rt:systest_read(Node1, 100, 1),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),
    ok.
    

