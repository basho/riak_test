%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(rt_ring).
-include_lib("eunit/include/eunit.hrl").

-export([assert_nodes_agree_about_ownership/1,
         check_singleton_node/1,
         claimant_according_to/1,
         get_ring/1,
         members_according_to/1,
         nearest_ringsize/1,
         nearest_ringsize/2,
         owners_according_to/1,
         partitions_for_node/1,
         status_of_according_to/2]).

%% @doc Ensure that the specified node is a singleton node/cluster -- a node
%%      that owns 100% of the ring.
check_singleton_node(Node) ->
    lager:info("Check ~p is a singleton", [Node]),
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Owners = lists:usort([Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)]),
    ?assertEqual([Node], Owners),
    ok.

% @doc Get list of partitions owned by node (primary).
partitions_for_node(Node) ->
    Ring = get_ring(Node),
    [Idx || {Idx, Owner} <- riak_core_ring:all_owners(Ring), Owner == Node].

%% @doc Get the raw ring for `Node'.
get_ring(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Ring.

assert_nodes_agree_about_ownership(Nodes) ->
    ?assertEqual(ok, rt:wait_until_ring_converged(Nodes)),
    ?assertEqual(ok, rt:wait_until_all_members(Nodes)),
    [ ?assertEqual({Node, Nodes}, {Node, owners_according_to(Node)}) || Node <- Nodes].

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
owners_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
            lists:usort(Owners);
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Return a list of cluster members according to the ring retrieved from
%%      the specified node.
members_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Members = riak_core_ring:all_members(Ring),
            Members;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Return an appropriate ringsize for the node count passed
%%      in. 24 is the number of cores on the bigger intel machines, but this
%%      may be too large for the single-chip machines.
nearest_ringsize(Count) ->
    nearest_ringsize(Count * 24, 2).

nearest_ringsize(Count, Power) ->
    case Count < trunc(Power * 0.9) of
        true ->
            Power;
        false ->
            nearest_ringsize(Count, Power * 2)
    end.

%% @doc Return the cluster status of `Member' according to the ring
%%      retrieved from `Node'.
status_of_according_to(Member, Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Status = riak_core_ring:member_status(Ring, Member),
            Status;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
claimant_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Claimant = riak_core_ring:claimant(Ring),
            Claimant;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.
