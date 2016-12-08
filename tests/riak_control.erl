%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%
%% @doc Run Riak Control on all nodes, and verify that we can upgrade
%%      from legacy and previous to current, while ensuring Riak Control
%%      continues to operate and doesn't crash on any node.

-module(riak_control).

-behaviour(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(RC_ENABLE_CFG, [{riak_control, [{enabled, true}, {auth, none}]}]).

%% @doc Verify that Riak Control operates predictably during an upgrade.
confirm() ->
    verify_upgrade(legacy),
    rt:setup_harness(ignored, ignored),
    verify_upgrade(previous),
    rt:setup_harness(ignored, ignored),
    pass.

%% @doc Verify an upgrade succeeds with all nodes running control from
%%      the specified `Vsn' to current.
verify_upgrade(Vsn) ->
    lager:info("Verify upgrade from ~p to current.", [Vsn]),

    lager:info("Building cluster."),
    [Nodes] = rt:build_clusters([{3, Vsn, ?RC_ENABLE_CFG}]),

    lager:info("Verifying all nodes are alive."),
    verify_alive(Nodes),

    lager:info("Upgrading each node and verifying Control."),
    VersionedNodes = [{Vsn, Node} || Node <- Nodes],
    lists:foldl(fun verify_upgrade_fold/2, VersionedNodes, VersionedNodes),

    lager:info("Validate capability convergence."),
    validate_capability(VersionedNodes),

    ok.

%% @doc Verify upgrade fold function.
verify_upgrade_fold({FromVsn, Node}, VersionedNodes0) ->
    lager:info("Upgrading ~p from ~p to current.", [Node, FromVsn]),

    lager:info("Performing upgrade."),
    rt:upgrade(Node, current),
    rt:wait_for_service(Node, riak_kv),

    %% Wait for Riak Control to start.
    rt:wait_for_control(VersionedNodes0),

    %% Wait for Riak Control polling cycle.
    wait_for_control_cycle(Node),

    lager:info("Versioned nodes is: ~p.", [VersionedNodes0]),
    VersionedNodes = lists:keyreplace(Node, 2, VersionedNodes0, {current, Node}),
    lager:info("Versioned nodes is now: ~p.", [VersionedNodes]),

    lager:info("Verify that all nodes are still alive."),
    verify_alive([VersionedNode || {_, VersionedNode} <- VersionedNodes]),

    lager:info("Verify that control still works on all nodes."),
    verify_control(VersionedNodes),

    VersionedNodes.

verify_control({Vsn, Node}, VersionedNodes) ->
    lager:info("Verifying control on node ~p vsn ~p.", [Node, Vsn]),

    %% Verify node resource.
    ?assertMatch(ok,
        rt:wait_until(
            fun() ->
                validate_nodes(Node, VersionedNodes, any)
            end
        )
    ),
    ?assertMatch(ok,
        rt:wait_until(
            fun() ->
                validate_partitions({Vsn, Node}, VersionedNodes)
            end
        )
    ),
    ok.

verify_control(VersionedNodes) ->
    [verify_control(NodeVsn, VersionedNodes) || NodeVsn <- VersionedNodes].

%% @doc Verify a particular JSON resource responds.
verify_resource(Node0, Resource) ->
    Node = rt:http_url(Node0),
    Output = os:cmd(io_lib:format("curl -s -S ~s~p", [Node, Resource])),
    lager:info("Verifying node ~p resource ~p.", [Node, Resource]),
    mochijson2:decode(Output).

%% @doc Verify that riak_kv is still running on all nodes.
verify_alive(Nodes) ->
    [rt:wait_for_service(Node, riak_kv) || Node <- Nodes].

%% @doc This section iterates over the JSON response of nodes, and
%%      verifies that each node is reporting its status correctly based
%%      on it's current Vsn.
validate_nodes(ControlNode, VersionedNodes, Status0) ->
    {struct,
        [{<<"nodes">>, ResponseNodes}]} = verify_resource(ControlNode, "/admin/nodes"),
    MixedCluster = mixed_cluster(VersionedNodes),
    lager:info("Mixed cluster: ~p.", [MixedCluster]),

    Results = lists:map(fun({struct, Node}) ->

                %% Parse JSON further.
                BinaryName = proplists:get_value(<<"name">>, Node),
                Status = proplists:get_value(<<"status">>, Node),
                Name = list_to_existing_atom(binary_to_list(BinaryName)),

                %% Find current Vsn of node we are validating, and the
                %% vsn of the node running Riak Control that we've
                %% queried.
                {NodeVsn, _} = lists:keyfind(Name, 2, VersionedNodes),
                {ControlVsn, _} = lists:keyfind(ControlNode, 2, VersionedNodes),

                %% Determine what the correct status should be, or if
                %% we've been told to test a specific status, use that.
                case Status0 of
                    any ->
                        valid_status(MixedCluster, ControlVsn, NodeVsn, Status);
                    _ ->
                        Status0 =:= Status
                end
        end, ResponseNodes),
    lists:all(fun(Result) -> Result end, Results).

%% @doc Determine if we're currently running mixed mode.
mixed_cluster(VersionedNodes) ->
    length(lists:usort(
            lists:map(fun({Vsn, _}) -> Vsn end, VersionedNodes))) =/= 1.

wait_for_control_cycle(Node) when is_atom(Node) ->
    lager:info("Waiting for riak_control poll on node ~p.", [Node]),

    {ok, CurrentVsn} = rpc:call(Node,
                                riak_control_session,
                                get_version,
                                []),
    ExpectedVsn = CurrentVsn + 1,

    rt:wait_until(Node, fun(N) ->
                {ok, Vsn} = rpc:call(N,
                                     riak_control_session,
                                     get_version,
                                     []),
                Vsn =:= ExpectedVsn
        end).

%% @doc Validate partitions response.
validate_partitions({ControlVsn, ControlNode}, VersionedNodes) ->
    %% Verify partitions resource.
    VersionBinary = rt:get_version(ControlVsn),
    ResponsePartitions = case VersionBinary of
                     <<"riak_ee-2.", _/binary>> ->
                         {struct,
                             [{<<"partitions">>, NodePartitions},
                                 {<<"default_n_val">>, _}]} = verify_resource(ControlNode, "/admin/partitions"),
                         NodePartitions;
                     <<"riak-2.", _/binary>> ->
                         {struct,
                             [{<<"partitions">>, NodePartitions},
                                 {<<"default_n_val">>, _}]} = verify_resource(ControlNode, "/admin/partitions"),
                         NodePartitions;
                     <<"riak_ts", _/binary>> ->
                         {struct,
                              [{<<"partitions">>, NodePartitions},
                                  {<<"default_n_val">>, _}]} = verify_resource(ControlNode, "/admin/partitions"),
                             NodePartitions;
                     _ ->
                         {struct,
                             [{<<"partitions">>, NodePartitions}]} = verify_resource(ControlNode, "/admin/partitions"),
                         NodePartitions
                 end,
    %% The newest version of the partitions display can derive the
    %% partition state without relying on data from rpc calls -- it can
    %% use just the ring to do this.  Don't test anything specific here
    %% yet.
    case VersionBinary of
        <<"riak_ee-2.", _/binary>> ->
            true;
        <<"riak-2.", _/binary>> ->
            true;
        <<"riak_ts", _/binary>> ->
            true;
        _ ->
            MixedCluster = mixed_cluster(VersionedNodes),
            lager:info("Mixed cluster: ~p.", [MixedCluster]),

            Results = lists:map(fun({struct, Partition}) ->

                        %% Parse JSON further.
                        BinaryName = proplists:get_value(<<"node">>, Partition),
                        Status = proplists:get_value(<<"status">>, Partition),
                        Name = list_to_existing_atom(binary_to_list(BinaryName)),

                        %% Find current Vsn of node we are validating, and the
                        %% vsn of the node running Riak Control that we've
                        %% queried.
                        {NodeVsn, _} = lists:keyfind(Name, 2, VersionedNodes),
                        valid_status(MixedCluster, ControlVsn, NodeVsn, Status)
                end, ResponsePartitions),
            lists:all(fun(Result) -> Result end, Results)
    end.

%% @doc Validate status based on Vsn.
valid_status(false, current, current, <<"valid">>) ->
    %% Fully upgraded cluster, already negotiated.
    true;
valid_status(true, _, _, <<"valid">>) ->
    %% Cross-version communication in mixed cluster.
    true;
valid_status(MixedCluster, ControlVsn, NodeVsn, Status) ->
    %% Default failure case.
    lager:info("Invalid status: ~p ~p ~p ~p", [MixedCluster,
                                               ControlVsn,
                                               NodeVsn,
                                               Status]),
    false.

%% @doc Validate capability has converged.
validate_capability(VersionedNodes) ->
    %% Wait for capability negotiation.
    [rt:wait_until_capability(Node,
                              {riak_control, member_info_version},
                              v1) || {_, Node} <- VersionedNodes],

    %% We can test any node here, so just choose the first.
    [{_Vsn, Node}|_] = VersionedNodes,
    lager:info("Verifying capability through ~p.", [Node]),

    %% Wait the Riak Control converges.
    lager:info("Waiting for riak_control to converge."),

    rt:wait_until(Node, fun(N) ->
                {ok, _, Status} = rpc:call(N,
                                           riak_control_session,
                                           get_status,
                                           []),
                Status =:= valid
        end),


    %% Validate we are in the correct state, not the incompatible state,
    %% which ensure the capability has negotiated correctly.
    validate_nodes(Node, VersionedNodes, <<"valid">>).
