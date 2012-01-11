-module(verify_leave).
-export([verify_leave/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [build_cluster/1,
             leave/1,
             wait_until_unpingable/1,
             owners_according_to/1,
             status_of_according_to/2,
             remove/2]).

verify_leave() ->
    %% Bring up a 3-node cluster for the test
    Nodes = build_cluster(3),
    [Node1, Node2, Node3] = Nodes,

    %% Have node2 leave
    lager:info("Have ~p leave", [Node2]),
    leave(Node2),
    ?assertEqual(ok, wait_until_unpingable(Node2)),

    %% Verify node2 no longer owns partitions, all node believe it invalid
    lager:info("Verify ~p no longer owns partitions and all nodes believe "
               "it is invalid", [Node2]),
    Remaining1 = Nodes -- [Node2],
    [?assertEqual(Remaining1, owners_according_to(Node)) || Node <- Remaining1],
    [?assertEqual(invalid, status_of_according_to(Node2, Node)) || Node <- Remaining1],

    %% Have node1 remove node3
    lager:info("Have ~p remove ~p", [Node1, Node3]),
    remove(Node1, Node3),
    ?assertEqual(ok, wait_until_unpingable(Node3)),

    %% Verify node3 no longer owns partitions, all node believe it invalid
    lager:info("Verify ~p no longer owns partitions, and all nodes believe "
               "it is invalid", [Node3]),
    Remaining2 = Remaining1 -- [Node3],
    [?assertEqual(Remaining2, owners_according_to(Node)) || Node <- Remaining2],
    [?assertEqual(invalid, status_of_according_to(Node3, Node)) || Node <- Remaining2],
    ok.
