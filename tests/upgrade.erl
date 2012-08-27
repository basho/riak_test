-module(upgrade).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Nodes = rt:build_cluster(["1.0.3", "1.0.3", "1.1.4", current]),
    [Node1, Node2, Node3, _Node4] = Nodes,

    lager:info("Writing 100 keys"),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),
    rt:upgrade(Node1, current),
    lager:info("Ensuring keys still exist"),
    rt:stop(Node2),
    rt:stop(Node3),
    rt:systest_read(Node1, 100, 1),
    %% ?assertEqual([], rt:systest_read(Node1, 100, 1)),
    wait_until_readable(Node1, 100),
    pass.

wait_until_readable(Node, N) ->
    rt:wait_until(Node,
                  fun(_) ->
                          [] == rt:systest_read(Node, N, 1)
                  end).


