-module(location_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

-import(location, [setup_location/2]).

-define(N_VAL, 3).

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),
    Nodes = [Node1, Node2] = rt:build_cluster([OldVsn, OldVsn]),

    Claimant = rt:claimant_according_to(Node1),

    lager:info("Upgrading claimant node"),
    upgrade(Claimant, current),

    setup_location(Nodes, #{Claimant => "test_location"}),

    lager:info("Upgrading all nodes"),
    [rt:upgrade(Node, current) || Node <- Nodes, Node =/= Claimant],

    ?assertEqual([], riak_core_location:check_ring(rt:get_ring(Node1), ?N_VAL, 2)),

    setup_location(Nodes, #{Node1 => "node1_location",
                            Node2 => "This_is_the_node2's_location"}),

    lager:info("Downgrading all nodes"),
    [rt:upgrade(Node, previous) || Node <- Nodes, Node =/= Claimant],

    lager:info("Test verify location upgrade test: Passed"),
    pass.


upgrade(Node, NewVsn) ->
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, [riak_kv]),
    ok.
