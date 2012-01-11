-module(verify_claimant).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [build_cluster/1,
             start/1,
             stop/1,
             down/2,
             claimant_according_to/1,
             wait_until_unpingable/1,
             wait_until_ring_converged/1,
             status_of_according_to/2,
             wait_until_nodes_ready/1]).

verify_claimant() ->
    Nodes = build_cluster(3),
    [Node1, Node2, _Node3] = Nodes,

    %% Ensure all nodes believe node1 is the claimant
    lager:info("Ensure all nodes believe ~p is the claimant", [Node1]),
    [?assertEqual(Node1, claimant_according_to(Node)) || Node <- Nodes],

    %% Stop node1
    lager:info("Stop ~p", [Node1]),
    stop(Node1),
    ?assertEqual(ok, wait_until_unpingable(Node1)),

    %% Ensure all nodes still believe node1 is the claimant
    lager:info("Ensure all nodes still believe ~p is the claimant", [Node1]),
    Remaining = Nodes -- [Node1],
    [?assertEqual(Node1, claimant_according_to(Node)) || Node <- Remaining],

    %% Mark node1 as down and wait for ring convergence
    lager:info("Mark ~p as down", [Node1]),
    down(Node2, Node1),
    ?assertEqual(ok, wait_until_ring_converged(Remaining)),
    [?assertEqual(down, status_of_according_to(Node1, Node)) || Node <- Remaining],

    %% Ensure all nodes now believe node2 to be the claimant
    lager:info("Ensure all nodes now believe ~p is the claimant", [Node2]),
    [?assertEqual(Node2, claimant_according_to(Node)) || Node <- Remaining],

    %% Restart node1 and wait for ring convergence
    lager:info("Restart ~p and wait for ring convergence", [Node1]),
    start(Node1),
    ?assertEqual(ok, wait_until_nodes_ready([Node1])),
    ?assertEqual(ok, wait_until_ring_converged(Nodes)),

    %% Ensure node has rejoined and is no longer down
    lager:info("Ensure ~p has rejoined and is no longer down", [Node1]),
    [?assertEqual(valid, status_of_according_to(Node1, Node)) || Node <- Nodes],

    %% Ensure all nodes still believe node2 is the claimant
    lager:info("Ensure all nodes still believe ~p is the claimant", [Node2]),
    [?assertEqual(Node2, claimant_according_to(Node)) || Node <- Nodes],
    ok.
