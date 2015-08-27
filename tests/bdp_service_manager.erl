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

-module(bdp_service_manager).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(S1_NAME, "service-one").
-define(S1_TYPE, "cache-proxy").
%% (this is parameter 'Group' in various calls to data_platform_global_state functions)
-define(S1_CONFIG, [{{"CACHE_PROXY_PORT","11211"},
                     {"CACHE_PROXY_STATS_PORT","22123"},
                     {"CACHE_TTL","15s"},
                     {"HOST","0.0.0.0"},
                     {"REDIS_SERVERS","127.0.0.1:6379"},
                     {"RIAK_KV_SERVERS","127.0.0.1:8087"}}]).

confirm() ->
    NumNodes = 3,
    lager:info("Building cluster and waiting for ensemble to stablize"),
    [Node1, Node2, Node3] = build_cluster(NumNodes),

    %% add a service
    ok = service_added(Node1, ?S1_NAME, ?S1_TYPE, ?S1_CONFIG),
    ok = wait_services(Node1, {[], [?S1_NAME]}),
    lager:info("Service ~p (~s) added", [?S1_NAME, ?S1_TYPE]),

    %% 1st arg is the node the service is configured to run,
    %% start/stop to be called from nodes given in args 2/3
    ok = test_cross_node_start_stop(Node1, Node1, Node2),
    ok = test_cross_node_start_stop(Node1, Node1, Node1),
    ok = test_cross_node_start_stop(Node1, Node2, Node2),
    ok = test_cross_node_start_stop(Node1, Node3, Node1),

    ok = test_cross_node_start_stop(Node2, Node1, Node2),
    ok = test_cross_node_start_stop(Node2, Node1, Node1),
    ok = test_cross_node_start_stop(Node2, Node2, Node2),
    ok = test_cross_node_start_stop(Node2, Node3, Node1),

    ok = remove_service(Node2, ?S1_NAME),
    lager:info("Service removed"),

    pass.

%% copied from ensemble_util.erl
build_cluster(N) ->
    Nodes = rt:deploy_nodes(N),
    Node = hd(Nodes),
    rt:join_cluster(Nodes),
    ensemble_util:wait_until_cluster(Nodes),
    ensemble_util:wait_for_membership(Node),
    ensemble_util:wait_until_stable(Node, N),
    Nodes.


get_services(Node) ->
    {Running_, Available_} =
        rpc:call(Node, data_platform_global_state, services, []),
    {Running, Available} =
        {lists:sort([SName || {_Type, SName, _Node} <- Running_]),
         lists:sort([SName || {SName, _Type, _Conf} <- Available_])},
    lager:debug("Services running: ~p, available: ~p", [Running, Available]),
    {Running, Available}.

wait_services(Node, Services) ->
    wait_services_(Node, Services, 20).
wait_services_(_Node, _Services, SecsToWait) when SecsToWait =< 0 ->
    {error, services_not_ready};
wait_services_(Node, Services, SecsToWait) ->
    case get_services(Node) of
        Services ->
            ok;
        _Incomplete ->
            timer:sleep(1000),
            wait_services_(Node, Services, SecsToWait - 1)
    end.

add_service(Node, ConfigName, ServiceType, Config) ->
    {Rnn0, Avl0} = get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, add_service_config,
                  [ConfigName, ServiceType, Config, false]),
    Avl1 = lists:usort(Avl0 ++ [ConfigName]),
    ok = wait_services(Node, {Rnn0, Avl1}).


remove_service(Node, ConfigName) ->
    {Rnn0, Avl0} = get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, remove_service,
                  [ConfigName]),
    Avl1 = lists:usort(Avl0 -- [ConfigName]),
    ok = wait_services(Node, {Rnn0, Avl1}).


start_seervice(Node, ServiceNode, Group, ConfigName) ->
    {Rnn0, Avl0} = get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, start_service,
                  [Group, ConfigName, ServiceNode]),
    Rnn1 = lists:usort(Rnn0 ++ [ConfigName]),
    ok = wait_services(Node, {Rnn1, Avl0}).


stop_service(Node, ServiceNode, Group, ConfigName) ->
    {Rnn0, Avl0} = get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, stop_service,
                  [Group, ConfigName, ServiceNode]),
    Rnn1 = lists:usort(Rnn0 -- [ConfigName]),
    ok = wait_services(Node, {Rnn1, Avl0}).


test_cross_node_start_stop(ServiceNode, Node1, Node2) ->
    %% start it, on Node1
    ok = start_seervice(Node1, ServiceNode, ?S1_TYPE, ?S1_NAME),
    lager:info("Service ~p started on ~p", [?S1_NAME, Node1]),

    %% stop it, on Node2
    ok = stop_service(Node2, ServiceNode, ?S1_TYPE, ?S1_NAME),
    lager:info("Service ~p stopped on ~p", [?S1_NAME, Node2]),
    ok.
