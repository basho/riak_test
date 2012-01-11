-module(verify_down).
-export([verify_down/0]).
-include_lib("eunit/include/eunit.hrl").

verify_down() ->
    Nodes = rt:deploy_nodes(3),
    [Node1, Node2, Node3] = Nodes,

    %% Join node2 to node1 and wait for cluster convergence
    lager:info("Join ~p to ~p", [Node2, Node1]),
    rt:join(Node2, Node1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1, Node2])),
    ?assertEqual(ok, rt:wait_until_no_pending_changes([Node1, Node2])),

    %% Shutdown node2
    lager:info("Stopping ~p", [Node2]),
    rt:stop(Node2),
    ?assertEqual(ok, rt:wait_until_unpingable(Node2)),
    Remaining = Nodes -- [Node2],

    %% Join node3 to node1
    lager:info("Join ~p to ~p", [Node3, Node1]),
    rt:join(Node3, Node1),
    ?assertEqual(ok, rt:wait_until_all_members(Remaining, [Node3])),

    %% Ensure node3 remains in the joining state
    lager:info("Ensure ~p remains in the joining state", [Node3]),
    [?assertEqual(joining, rt:status_of_according_to(Node3, Node)) || Node <- Remaining],

    %% Mark node2 as down and wait for ring convergence
    lager:info("Mark ~p as down", [Node2]),
    rt:down(Node1, Node2),
    ?assertEqual(ok, rt:wait_until_ring_converged(Remaining)),
    [?assertEqual(down, rt:status_of_according_to(Node2, Node)) || Node <- Remaining],

    %% Ensure node3 is now valid
    [?assertEqual(valid, rt:status_of_according_to(Node3, Node)) || Node <- Remaining],

    %% Restart node2 and wait for ring convergence
    lager:info("Restart ~p and wait for ring convergence", [Node2]),
    rt:start(Node2),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node2])),
    ?assertEqual(ok, rt:wait_until_ring_converged(Nodes)),

    %% Verify that all three nodes are ready
    lager:info("Ensure all nodes are ready"),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ok.
