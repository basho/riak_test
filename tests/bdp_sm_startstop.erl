%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%% @doc A module to test BDP service manager operations: add a
%%      service, then perform a start/stop cycle on various
%%      combinations of starting, stopping and execution nodes

-module(bdp_sm_startstop).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(S1_NAME, "service-one").
-define(S1_TYPE, "cache-proxy").
-define(S1_CONFIG, [{"CACHE_PROXY_PORT","11211"},
                    {"CACHE_PROXY_STATS_PORT","22123"},
                    {"CACHE_TTL","15s"},
                    {"HOST","0.0.0.0"},
                    {"REDIS_SERVERS","127.0.0.1:6379"},
                    {"RIAK_KV_SERVERS","127.0.0.1:8087"}]).

confirm() ->
    ClusterSize = 3,
    lager:info("Building cluster"),
    _Nodes = [Node1, Node2, Node3] = bdp_util:build_cluster(ClusterSize),

    %% add a service
    ok = bdp_util:service_added(Node1, ?S1_NAME, ?S1_TYPE, ?S1_CONFIG),
    ok = bdp_util:wait_services(Node1, {[], [?S1_NAME]}),
    lager:info("Service ~p (~s) added", [?S1_NAME, ?S1_TYPE]),

    ok = test_service_manager(Node1, Node1, Node1, "111"),
    ok = test_service_manager(Node1, Node2, Node2, "122"),
    ok = test_service_manager(Node1, Node2, Node3, "123"),

    %% make up your mind on exactly how thoroughly we want to remove the node:
    %% * just leave the root ensemble;
    %% * same, and also leave the cluster;
    %% * same, and/or down the node.
    %% Then, rework the make_node_leave and add the various
    %% rt:wait_until checks accordingly.

    %% ok = bdp_util:make_node_leave(Node2, Node1),
    %% rt:remove(Node1, Node2),
    %% rt:start_and_wait(Node2),
    %% %rt:join_cluster(Nodes),
    %% ensemble_util:wait_until_cluster(Nodes),
    %% ok = bdp_util:make_node_join(Node2, Node1),
    %% ensemble_util:wait_until_cluster(Nodes),

    ok = test_service_manager(Node1, Node1, Node1, "111"),
    ok = test_service_manager(Node1, Node2, Node2, "122"),
    ok = test_service_manager(Node1, Node2, Node3, "123"),

    ok = bdp_util:service_removed(Node2, ?S1_NAME),
    ok = bdp_util:wait_services(Node1, {[], []}),
    lager:info("Service removed"),

    pass.



%% Use three nodes in various combinations to assign a service to run
%% on, to execute a start from, and execute stop.
test_service_manager(NodeA, NodeB, NodeC, Desc) ->
    lager:info("Battery ~p: starting", [Desc]),
    %% 1st arg is the node the service is configured to run,
    %% start/stop to be called from nodes given in args 2/3
    ok = test_cross_node_start_stop(NodeA, NodeA, NodeB),
    ok = test_cross_node_start_stop(NodeA, NodeA, NodeA),
    ok = test_cross_node_start_stop(NodeA, NodeB, NodeB),
    ok = test_cross_node_start_stop(NodeA, NodeC, NodeA),
    lager:info("Battery ~p: midway", [Desc]),

    ok = test_cross_node_start_stop(NodeB, NodeA, NodeB),
    ok = test_cross_node_start_stop(NodeB, NodeA, NodeA),
    ok = test_cross_node_start_stop(NodeB, NodeB, NodeB),
    ok = test_cross_node_start_stop(NodeB, NodeC, NodeA),
    lager:info("Battery ~p: completed", [Desc]),
    ok.


test_cross_node_start_stop(ServiceNode, Node1, Node2) ->
    %% start it, on Node1
    ok = bdp_util:service_started(Node1, ServiceNode, ?S1_NAME, ?S1_TYPE),
    lager:info("Service ~p up   on ~p", [?S1_NAME, Node1]),

    %% stop it, on Node2
    ok = bdp_util:service_stopped(Node2, ServiceNode, ?S1_NAME, ?S1_TYPE),
    lager:info("Service ~p down on ~p", [?S1_NAME, Node2]),
    ok.
