%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(verify_build_cluster).
-export([properties/0,
         confirm/1]).
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

properties() ->
    UpdConfig = rt_cluster:augment_config(riak_core,
                                          {default_bucket_props, [{allow_mult, false}]},
                                          rt_properties:default_config()),
    rt_properties:new([{config, UpdConfig},
                       {node_count, 4},
                       {rolling_upgrade, true},
                       {make_cluster, false},
                       {start_version, previous}]).

-spec confirm(rt_properties:properties()) -> pass | fail.
confirm(Properties) ->
    [Node1, Node2, Node3, Node4] = Nodes = rt_properties:get(nodes, Properties),

    lager:info("Loading some data up in this cluster."),
    ?assertEqual([], rt_systest:write(Node1, 0, 1000, <<"verify_build_cluster">>, 2)),

    lager:info("joining Node 2 to the cluster... It takes two to make a thing go right"),
    rt_node:join(Node2, Node1),
    wait_and_validate([Node1, Node2]),

    lager:info("joining Node 3 to the cluster"),
    rt_node:join(Node3, Node1),
    wait_and_validate([Node1, Node2, Node3]),

    lager:info("joining Node 4 to the cluster"),
    rt_node:join(Node4, Node1),
    wait_and_validate(Nodes),

    lager:info("taking Node 1 down"),
    rt_node:stop(Node1),
    ?assertEqual(ok, rt:wait_until_unpingable(Node1)),
    wait_and_validate(Nodes, [Node2, Node3, Node4]),

    lager:info("taking Node 2 down"),
    rt_node:stop(Node2),
    ?assertEqual(ok, rt:wait_until_unpingable(Node2)),
    wait_and_validate(Nodes, [Node3, Node4]),

    lager:info("bringing Node 1 up"),
    rt_node:start(Node1),
    ok = rt:wait_until_pingable(Node1),
    wait_and_validate(Nodes, [Node1, Node3, Node4]),
    lager:info("bringing Node 2 up"),
    rt_node:start(Node2),
    ok = rt:wait_until_pingable(Node2),
    wait_and_validate(Nodes),

    % leave 1, 2, and 3
    lager:info("leaving Node 1"),
    rt_node:leave(Node1),
    ?assertEqual(ok, rt:wait_until_unpingable(Node1)),
    wait_and_validate([Node2, Node3, Node4]),

    lager:info("leaving Node 2"),
    rt_node:leave(Node2),
    ?assertEqual(ok, rt:wait_until_unpingable(Node2)),
    wait_and_validate([Node3, Node4]),

    lager:info("leaving Node 3"),
    rt_node:leave(Node3),
    ?assertEqual(ok, rt:wait_until_unpingable(Node3)),

    % verify 4
    wait_and_validate([Node4]),

    pass.

wait_and_validate(Nodes) -> wait_and_validate(Nodes, Nodes).
wait_and_validate(RingNodes, UpNodes) ->
    lager:info("Wait until all nodes are ready and there are no pending changes"),
    ?assertEqual(ok, rt_node:wait_until_nodes_ready(UpNodes)),
    ?assertEqual(ok, rt:wait_until_all_members(UpNodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(UpNodes)),
    lager:info("Ensure each node owns a portion of the ring"),
    [rt_node:wait_until_owners_according_to(Node, RingNodes) || Node <- UpNodes],
    [rt:wait_for_service(Node, riak_kv) || Node <- UpNodes],
    lager:info("Verify that you got much data... (this is how we do it)"),
    ?assertEqual([], rt_systest:read(hd(UpNodes), 0, 1000, <<"verify_build_cluster">>, 2)),
    done.
