%% -------------------------------------------------------------------
%%% @copyright (C) 2017, NHS Digital
%%% @doc
%%% riak_test for node coverage participation behaviour.
%%%
%%% When a riak node is rebuilding its data it will still participate in
%%% coverage queries, returning the wrong answer as it does not yet have
%%% the full object indexes. To fix this, a manual operator's workaround
%%% is provided via a config option that prevents a node participating in
%%% coverage queries.
%%%
%%% This test demonstrates that this config option is communicated on the
%%% ring, and that a node does not participate in the coverage query when
%%% this option is specified.
%%%
%%% @end

-module(coverage_participation).
-behavior(riak_test).
-compile([export_all, nowarn_export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"bucket">>).
-define(KEY, <<"key">>).
-define(RING_SIZE, 32).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, ?RING_SIZE},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}]}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],
    NoCoverageConf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, ?RING_SIZE},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}
                                                 ]},
                                                 {participate_in_coverage, false}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],
    CoverageConf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, ?RING_SIZE},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}
                                                 ]},
                                                 {participate_in_coverage, true}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],

    %% Build cluster with default config (will test that default is 'true')
    Nodes = rt:build_cluster(5, Conf),
    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    lager:info("Configure Node5 not to participate in coverage queries."),
    rt:update_app_config(Node5, NoCoverageConf),
    ?assertEqual(ok, rt:wait_until_all_members(Nodes)),

    %% Check participate_in_coverage value
    lager:info("Check participate is set in environment correctly for each node."),
    ?assertEqual({ok, true}, rt:rpc_get_env(Node1, [{riak_core, participate_in_coverage}])),
    ?assertEqual({ok, true}, rt:rpc_get_env(Node2, [{riak_core, participate_in_coverage}])),
    ?assertEqual({ok, true}, rt:rpc_get_env(Node3, [{riak_core, participate_in_coverage}])),
    ?assertEqual({ok, true}, rt:rpc_get_env(Node4, [{riak_core, participate_in_coverage}])),
    ?assertEqual({ok, false}, rt:rpc_get_env(Node5, [{riak_core, participate_in_coverage}])),

    %% Now wait until the ring has gossiped and stabilised
    rt:wait_until_ring_converged(Nodes),

    %% Get ring and check that participate_in_coverage=false is on ring for Node5
    lager:info("Check participate has been gossiped over the ring for Node1, and Node5 not in coverage."),
    Ring1 = rt:get_ring(Node1),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring1, Node1, participate_in_coverage)),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring1, Node2, participate_in_coverage)),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring1, Node3, participate_in_coverage)),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring1, Node4, participate_in_coverage)),
    ?assertEqual(false, riak_core_ring:get_member_meta(Ring1, Node5, participate_in_coverage)),

    lager:info("Check participate has been gossiped over the ring for Node5, and Node5 not in coverage."),
    Ring5 = rt:get_ring(Node5),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring5, Node1, participate_in_coverage)),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring5, Node2, participate_in_coverage)),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring5, Node3, participate_in_coverage)),
    ?assertEqual(true, riak_core_ring:get_member_meta(Ring5, Node4, participate_in_coverage)),
    ?assertEqual(false, riak_core_ring:get_member_meta(Ring5, Node5, participate_in_coverage)),

    %% Get coverage plan
    lager:info("Check that Node5 is not in coverage plan."),
    {CoverageVNodes1, _} = rpc:call(Node1, riak_core_coverage_plan, create_plan, [allup,1,1,1,riak_kv]),
    Vnodes1 = [ Node || { _ , Node } <- CoverageVNodes1],
    %% check Node5 is not in coverage plan
    ?assertEqual(false, lists:keysearch(Node5, 1, Vnodes1)),

    lager:info("Check that Node5 is not in another coverage plan."),
    {CoverageVNodes5, _} = rpc:call(Node5, riak_core_coverage_plan, create_plan, [allup,1,2,1,riak_kv]),
    Vnodes5 = [ Node || { _ , Node } <- CoverageVNodes5],
    %% check Node5 is not in coverage plan
    ?assertEqual(false, lists:member(Node5, Vnodes5)),

    lager:info("Configure Node5 to re-participate in coverage queries."),
    rt:update_app_config(Node5, CoverageConf),

    %% Now wait until the ring has gossiped and stabilised
    rt:wait_until_ring_converged(Nodes),

    lager:info("Check participate has been gossiped over the ring for Node5, and Node5 back in coverage."),
    CheckBackInFun = 
        fun(N) ->
            RingN = rt:get_ring(N),
            riak_core_ring:get_member_meta(RingN, Node5, participate_in_coverage)
        end,
    lists:foreach(fun(N0) -> rt:wait_until(N0, CheckBackInFun) end, Nodes),

    CheckInPlanFun =
        fun() ->
            %% Get coverage plan
            lager:info("Check that Node5 is in coverage plan."),
            {CoverageVNodesW5, _} =
                rpc:call(Node5,
                        riak_core_coverage_plan,
                        create_plan,
                        [allup,1,1,1,riak_kv]),
            VnodesW5 = [ Node || { _ , Node } <- CoverageVNodesW5],
            lists:member(Node5, VnodesW5)
        end,
    rt:wait_until(CheckInPlanFun),

    lager:info("Take Node5 out of coverage at run-time"),
    rpc:call(Node5, riak_client, remove_node_from_coverage, []),
    %% Now wait until the ring has gossiped and stabilised
    rt:wait_until_ring_converged(Nodes),

    %% Get coverage plan
    lager:info("Check that Node5 is not in coverage plan."),
    {CoverageVNodes1, _} = rpc:call(Node1, riak_core_coverage_plan, create_plan, [allup,1,1,1,riak_kv]),
    Vnodes1 = [ Node || { _ , Node } <- CoverageVNodes1],
    %% check Node5 is not in coverage plan
    ?assertEqual(false, lists:keysearch(Node5, 1, Vnodes1)),

    lager:info("Check that Node5 is not in another coverage plan."),
    {CoverageVNodes5, _} = rpc:call(Node5, riak_core_coverage_plan, create_plan, [allup,1,2,1,riak_kv]),
    Vnodes5 = [ Node || { _ , Node } <- CoverageVNodes5],
    %% check Node5 is not in coverage plan
    ?assertEqual(false, lists:member(Node5, Vnodes5)),

    lager:info("Re-add Node5 at runtime"),
    rpc:call(Node5, riak_client, reset_node_for_coverage, []),
    %% Now wait until the ring has gossiped and stabilised
    rt:wait_until_ring_converged(Nodes),

    lists:foreach(fun(N0) -> rt:wait_until(N0, CheckBackInFun) end, Nodes),
    rt:wait_until(CheckInPlanFun),

    pass.

n(Atom) ->
    atom_to_list(Atom).
