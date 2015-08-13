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
-module(verify_build_cluster_caps_race).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

%% We have to define our own deploy_nodes to force a race condition
-define(HARNESS, (rt_config:get(rt_harness))).


deploy_nodes(InitialConfig) ->
    NodeConfig = [{current, Config} || Config <- InitialConfig],
    Nodes = ?HARNESS:deploy_nodes(NodeConfig),
    lager:info("Start nodes ~p without waiting for services", [Nodes]),
    Nodes.

staged_join(InitiatingNode, DestinationNode) ->
    rpc:call(InitiatingNode, riak_core, staged_join,
             [DestinationNode]).

confirm() ->
    %% Deploy a set of new nodes
    lager:info("Deploying nodes"),

    %% We want riak_core to be slow to start on node 2 to verify that
    %% the join will be disallowed if init is not yet complete
    Configs = [
               [{riak_core, []}],
               [{riak_core, [{delayed_start, 20000}]}]
              ],

    [Node1, Node2] = deploy_nodes(Configs),

    lager:info("joining Node 2 to the cluster..."),
    ?assertMatch({error, _}, staged_join(Node2, Node1)),
    pass.
