-module(verify_build_cluster).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

cover_modules() ->
    [riak_core_claimant].

verify_build_cluster() ->
    %% Deploy a set of new nodes
    lager:info("Deploying 3 nodes"),
    Nodes = rt:deploy_nodes(3),

    ct_cover:add_nodes(Nodes),

    %% Ensure each node owns 100% of it's own ring
    lager:info("Ensure each nodes 100% of it's own ring"),
    [?assertEqual([Node], rt:owners_according_to(Node)) || Node <- Nodes],

    %% Join nodes
    lager:info("Join nodes together"),
    [Node1|OtherNodes] = Nodes,
    [rt:join(Node, Node1) || Node <- OtherNodes],

    timer:sleep(3000),
    lager:info("Wait until all nodes are ready and there are no pending changes"),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_ring_converged(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),

    %% Ensure each node owns a portion of the ring
    lager:info("Ensure each node owns a portion of the ring"),
    [?assertEqual(Nodes, rt:owners_according_to(Node)) || Node <- Nodes],
    lager:info("verify_build_cluster: PASS"),
    ok.
