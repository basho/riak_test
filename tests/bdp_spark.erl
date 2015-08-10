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
%% @doc A module to test Spark fail/recovery under BDP service manager.

-module(bdp_spark).
-behavior(riak_test).
-export([confirm/0]).

-define(SPARK_SERVICE_TYPE, "cache-proxy").
-define(SPARK_SERVICE_NAME, "spark-fail-recovery-test").
-define(SPARK_SERVICE_CONFIG, [{"CACHE_PROXY_PORT","11211"},
                               {"CACHE_PROXY_STATS_PORT","22123"},
                               {"CACHE_TTL","15s"},
                               {"HOST","0.0.0.0"},
                               {"REDIS_SERVERS","127.0.0.1:6379"},
                               {"RIAK_KV_SERVERS","127.0.0.1:8087"}]).

confirm() ->
    ClusterSize = 3,
    lager:info("Building cluster"),
    _Nodes = [Node1, _Node2, _Node3] = bdp_util:build_cluster(ClusterSize),

    %% add a service
    ok = service_added(Node1, ?SPARK_SERVICE_NAME, ?SPARK_SERVICE_TYPE, ?SPARK_SERVICE_CONFIG),
    ok = bdp_util:wait_services(Node1, {[], [?SPARK_SERVICE_NAME]}),
    lager:info("Service ~p (~s) added", [?SPARK_SERVICE_NAME, ?SPARK_SERVICE_TYPE]),

    ok = service_started(Node1, Node1, ?SPARK_SERVICE_NAME, ?SPARK_SERVICE_TYPE),
    lager:info("Service ~p up   on ~p", [?SPARK_SERVICE_NAME, Node1]),

    ok = test_spark_fail_recovery(),

    ok = service_stopped(Node1, Node1, ?SPARK_SERVICE_NAME, ?SPARK_SERVICE_TYPE),
    lager:info("Service ~p down on ~p", [?SPARK_SERVICE_NAME, Node1]),

    ok = service_removed(Node1, ?SPARK_SERVICE_NAME),
    ok = bdp_util:wait_services(Node1, {[], []}),
    lager:info("Service ~p removed", [?SPARK_SERVICE_NAME]),

    pass.


%% The following functions are our makeshift Service Manager API, with
%% some caveats:
%%
%% 1. The actual involvement of erlexec happens at ticks firing
%%    periodically (every 2 sec) within data_platform_manager, where
%%    reconcile_global_state/0 checks which services need to run.
%%    Hence we insert appropriate delays after each call.
%%
%% 2. Because we call the actual data_platform_global_state functions
%%    via RPC, we supply the node at which the calls are to be made,
%%    as the first arg.

-define(TICK_ALLOWANCE, 1000).
%% However, with realistic values (more precisely, after first firing
%% of the tick), all rpc calls return {error, timeout}.

service_added(Node, ServiceName, ServiceType, Config) ->
    {Rnn0, Avl0} = bdp_util:get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, add_service_config,
                  [ServiceName, ServiceType, Config, false]),
    Avl1 = lists:usort(Avl0 ++ [ServiceName]),
    ok = bdp_util:wait_services(Node, {Rnn0, Avl1}),
    timer:sleep(?TICK_ALLOWANCE),
    ok.

service_removed(Node, ServiceName) ->
    {Rnn0, Avl0} = bdp_util:get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, remove_service,
                  [ServiceName]),
    Avl1 = lists:usort(Avl0 -- [ServiceName]),
    ok = bdp_util:wait_services(Node, {Rnn0, Avl1}),
    timer:sleep(?TICK_ALLOWANCE),
    ok.


service_started(Node, ServiceNode, ServiceName, Group) ->
    {Rnn0, Avl0} = bdp_util:get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, start_service,
                  [Group, ServiceName, ServiceNode]),
    Rnn1 = lists:usort(Rnn0 ++ [ServiceName]),
    ok = bdp_util:wait_services(Node, {Rnn1, Avl0}),
    timer:sleep(?TICK_ALLOWANCE),
    ok.

service_stopped(Node, ServiceNode, ServiceName, Group) ->
    {Rnn0, Avl0} = bdp_util:get_services(Node),
    ok = rpc:call(Node, data_platform_global_state, stop_service,
                  [Group, ServiceName, ServiceNode]),
    Rnn1 = lists:usort(Rnn0 -- [ServiceName]),
    ok = bdp_util:wait_services(Node, {Rnn1, Avl0}),
    timer:sleep(?TICK_ALLOWANCE),
    ok.


test_spark_fail_recovery() ->
    %% execute $ ps -ef | grep org.apache.spark.deploy.master.Master
    %% to find out the PIDs of running masters and to make sure they are actually running
    %%
    %% To find out which of the masters is the leader open spark
    %% master log files located in the spark-master/logs if the line
    %%
    %% ‘INFO RiakEnsembleLeaderElectionAgent: We have lost leadership’
    %%
    %% is found as a last one from RiakEnsembleLeaderElectionAgent,
    %% this instance is currently a standby master. Let’s ‘tail -f’
    %% this log.  This way we’ll see when it’ll gain leadership

    %% Go to the instance that has and entry
    %% ‘INFO RiakEnsembleLeaderElectionAgent: We have gained leadership’
    %% and execute ‘kill <master’s pid>’

    %% Verify that in the standby master’s logs that you tail there are entries like
    %% 15/07/09 02:13:46 INFO RiakEnsembleLeaderElectionAgent: We have gained leadership
    %% 15/07/09 02:13:46 INFO Master: I have been elected leader! New state: RECOVERING
    ok.
