-module(verify_build_cluster).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/1,
             owners_according_to/1,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

verify_build_cluster() ->
    %% Deploy a set of new nodes
    lager:info("Deploying 3 nodes"),
    Nodes = deploy_nodes(3),

    %% Ensure each node owns 100% of it's own ring
    lager:info("Ensure each nodes 100% of it's own ring"),
    [?assertEqual([Node], owners_according_to(Node)) || Node <- Nodes],

    %% Join nodes
    lager:info("Join nodes together"),
    [Node1|OtherNodes] = Nodes,
    [join(Node, Node1) || Node <- OtherNodes],

    lager:info("Wait until all nodes are ready and there are no pending changes"),
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),

    %% Ensure each node owns a portion of the ring
    lager:info("Ensure each node owns a portion of the ring"),
    [?assertEqual(Nodes, owners_according_to(Node)) || Node <- Nodes],
    lager:info("verify_build_cluster: PASS"),
    ok.
