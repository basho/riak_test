-module(location).
-behavior(riak_test).
-export([confirm/0]).
-export([setup_location/2]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

-define(N_VAL, 3).

-define(RACK_A, "rack_a").
-define(RACK_B, "rack_b").
-define(RACK_C, "rack_c").
-define(RACK_D, "rack_d").
-define(RACK_E, "rack_e").
-define(RACK_F, "rack_f").

confirm() ->
    AllNodes = rt:deploy_nodes(6),
    [Node1, Node2, Node3, Node4, Node5, Node6] = AllNodes,
    Nodes = [Node1, Node2, Node3, Node4],

    rt:staged_join(Node2, Node1),
    rt:staged_join(Node3, Node1),
    rt:staged_join(Node4, Node1),

    % Set one location
    setup_location(Nodes, #{Node1 => ?RACK_A}),
    Ring1 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring1),

    % Change name of the location and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_B}),
    Ring2 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring2),
    assert_no_ownership_change(Ring1, Ring2),

    % Two Nodes same location
    setup_location(Nodes, #{Node1 => ?RACK_B,
                            Node2 => ?RACK_B}),
    assert_ring_satisfy_n_val(rt:get_ring(Node1)),

    % Two Nodes different Locations
    setup_location(Nodes, #{Node1 => ?RACK_A,
                            Node2 => ?RACK_B}),
    Ring3 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring3),

    % Change one of node location and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_C,
                            Node2 => ?RACK_B}),
    Ring4 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring4),
    assert_no_ownership_change(Ring4, Ring3),

    % Change both nodes locations and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_B,
                            Node2 => ?RACK_D}),
    assert_ring_satisfy_n_val(rt:get_ring(Node1)),

    % Three Nodes with different Locations
    setup_location(Nodes, #{Node1 => ?RACK_C,
                            Node2 => ?RACK_D,
                            Node3 => ?RACK_A}),
    Ring5 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring5),
    assert_no_location_violation(Ring5),

    % For Nodes with different Locations
    setup_location(Nodes, #{Node1 => ?RACK_C,
                            Node2 => ?RACK_D,
                            Node3 => ?RACK_A,
                            Node4 => ?RACK_B}),
    Ring6 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring6),
    assert_no_location_violation(Ring6),

    % Change all nodes locations and expect no transfers
    setup_location(Nodes, #{Node1 => ?RACK_A,
                            Node2 => ?RACK_B,
                            Node3 => ?RACK_C,
                            Node4 => ?RACK_D}),

    Ring7 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring7),
    assert_no_location_violation(Ring7),
    assert_no_ownership_change(Ring6, Ring7),

    rt:staged_join(Node5, Node1),

    setup_location(AllNodes, #{Node1 => ?RACK_A,
                               Node2 => ?RACK_B,
                               Node3 => ?RACK_C,
                               Node4 => ?RACK_B,
                               Node5 => ?RACK_A
    }),
    Ring8 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring8),

    rt:staged_join(Node6, Node1),

    setup_location(AllNodes, #{Node1 => ?RACK_A,
                               Node2 => ?RACK_A,
                               Node3 => ?RACK_B,
                               Node4 => ?RACK_B,
                               Node5 => ?RACK_C,
                               Node6 => ?RACK_C
                              }),
    Ring9 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring9),
    % Because of tail violations need to increase n_val to satisfy diversity of locations
    assert_no_location_violation(Ring9, 4, 3),

    setup_location(AllNodes, #{Node1 => ?RACK_A,
                               Node2 => ?RACK_B,
                               Node3 => ?RACK_C,
                               Node4 => ?RACK_C,
                               Node5 => ?RACK_B,
                               Node6 => ?RACK_A
    }),
    Ring10 = rt:get_ring(Node1),
    assert_ring_satisfy_n_val(Ring10),
    % Because of tail violations need to increase n_val to satisfy diversity of locations
    assert_no_location_violation(Ring10, 4, 3),

    lager:info("Test verify location settings: Passed"),
    pass.

-spec set_location(node(), string()) -> ok | {fail, term()}.
set_location(Node, Location) ->
    lager:info("Set ~p node location to ~p", [Node, Location]),
    JoinFun = fun() ->
        {ok, Result} = rt:admin(Node, ["cluster", "location", Location]),
        lists:prefix("Success:", Result)
    end,
    ok = rt:wait_until(JoinFun, 5, 1000).

-spec setup_location([node()], #{node() := string()}) -> ok.
setup_location([OnNode | _] = Nodes, NodeMap) ->
    maps:map(fun set_location/2, NodeMap),
    rt:plan_and_commit(OnNode),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)).

assert_ring_satisfy_n_val(Ring) ->
  lager:info("Ensure that every preflists satisfy n_val"),
  ?assertEqual([], riak_core_ring_util:check_ring(Ring, ?N_VAL)).

assert_no_ownership_change(RingA, RingB) ->
  lager:info("Ensure no ownership changed"),
  ?assertEqual(riak_core_ring:all_owners(RingA), riak_core_ring:all_owners(RingB)).

assert_no_location_violation(Ring) ->
  assert_no_location_violation(Ring, ?N_VAL, ?N_VAL).

assert_no_location_violation(Ring, NVal, MinNumberOfDistinctLocation) ->
  ?assertEqual(true, riak_core_location:has_location_set_in_cluster(riak_core_ring:get_nodes_locations(Ring))),
  log_assert_no_location_violation(NVal, MinNumberOfDistinctLocation),
  ?assertEqual([], riak_core_location:check_ring(Ring, NVal, MinNumberOfDistinctLocation)).

log_assert_no_location_violation(Nval, Nval) ->
  lager:info("Ensure that every preflists have uniq locations");
log_assert_no_location_violation(NVal, MinNumberOfDistinctLocation) ->
  lager:info("Ensure that every preflists (n_val: ~p) have at leaset ~p distinct locations",
             [NVal, MinNumberOfDistinctLocation]).
