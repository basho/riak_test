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

-include_lib("eunit/include/eunit.hrl").

-define(SERVICE_1, "spark-service-one").
-define(SERVICE_2, "spark-service-two").
-define(SPARK_MASTER_TYPE, "spark-master").
-define(SPARK_IDENT_STRING, "bdp_spark_test").
-define(HOSTNAME, "test").
-define(SPARK1_PID_DIR, "/tmp/service1").
-define(SPARK2_PID_DIR, "/tmp/service2").
-define(SERVICE_CONFIG_1, [{"SPARK_MASTER_PORT", "7077"}, {"HOST", "localhost"}, {"SPARK_PID_DIR", ?SPARK1_PID_DIR}, {"SPARK_IDENT_STRING", ?SPARK_IDENT_STRING}, {"HOSTNAME", ?HOSTNAME}]).
-define(SERVICE_CONFIG_2, [{"SPARK_MASTER_PORT", "7078"}, {"HOST", "localhost"}, {"SPARK_PID_DIR", ?SPARK2_PID_DIR}, {"SPARK_IDENT_STRING", ?SPARK_IDENT_STRING}, {"HOSTNAME", ?HOSTNAME}]).
-define(TIMEOUT, "60").

confirm() ->
    ClusterSize = 3,
    lager:info("Building cluster"),
    _Nodes = [Node1, Node2, _Node3] =
        bdp_util:build_cluster(ClusterSize),

    ok = create_spark_bucket_types(Node1),

    ok = add_spark_service(Node1, ?SERVICE_1, ?SERVICE_CONFIG_1),
    ok = add_spark_service(Node1, ?SERVICE_2, ?SERVICE_CONFIG_2),
    ok = start_services(Node1, [{Node1, ?SERVICE_1}, {Node2, ?SERVICE_2}]),

    ok = test_spark_fail_recovery(),

    ok = stop_services(Node1, [{Node1, ?SERVICE_1}, {Node2, ?SERVICE_2}]),
    ok = remove_spark_service(Node1, ?SERVICE_1),
    ok = remove_spark_service(Node1, ?SERVICE_2),

    pass.


create_spark_bucket_types(Node) ->
    rt:create_and_activate_bucket_type(Node, <<"strong">>, [{consistent, true}]),
    rt:create_and_activate_bucket_type(Node, <<"maps">>, [{datatype, map}]),
    lager:info("Spark bucket types 'strong' and 'maps' created"),
    ok.


add_spark_service(Node, ServiceName, Config) ->
    ok = bdp_util:add_service(Node, ServiceName, ?SPARK_MASTER_TYPE, Config),
    lager:info("Service definition ~p (~s) added to node ~p", [ServiceName, ?SPARK_MASTER_TYPE, Node]),
    ok.


start_services(Node, [{ServiceNode, ServiceName} | Rest]) ->
    ok = bdp_util:start_seervice(Node, ServiceNode, ServiceName, ?SPARK_MASTER_TYPE),
    lager:info("Service ~p up on ~p node", [ServiceName, ServiceNode]),
    start_services(Node, Rest);
start_services(_, []) -> ok.


stop_services(Node, [{ServiceNode, ServiceName} | Rest]) ->
    ok = bdp_util:stop_service(Node, ServiceNode, ServiceName, ?SPARK_MASTER_TYPE),
    lager:info("Service ~p down on ~p", [ServiceName, Node]),
    stop_services(Node, Rest);
stop_services(_, []) -> ok.


remove_spark_service(Node, ServiceName) ->
    ok = bdp_util:remove_service(Node, ServiceName),
    lager:info("Service ~p removed from ~p", [ServiceName, Node]),
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

    [DevPath | _] = rtdev:devpaths(),

    Node1Path = DevPath ++ "/dev/dev" ++ integer_to_list(1),
    Node2Path = DevPath ++ "/dev/dev" ++ integer_to_list(2),

    lager:info("Node1 path = ~s, Node2 path = ~s", [Node1Path, Node2Path]),

    LogSubPath = "/lib/data_platform*/priv/spark-master/logs/spark-" ++ ?SPARK_IDENT_STRING ++ "-org.apache.spark.deploy.master.Master-1-" ++ ?HOSTNAME ++  ".out",
    Spark1LogFile = Node1Path ++ LogSubPath,
    Spark2LogFile = Node2Path ++ LogSubPath,

    lager:info("Spark service one log path ~s", [Spark1LogFile]),
    lager:info("Spark service two log path ~s", [Spark2LogFile]),
 
    os:cmd("chmod +x ./priv/bdp_spark_test/leader_election_check.sh"),
    Command = "./priv/bdp_spark_test/leader_election_check.sh " ++ Spark1LogFile ++ " " ++ Spark2LogFile ++ " " ++ ?TIMEOUT,
    lager:info("Running bash script: ~s", [Command]),
    Res = os:cmd(Command),
    ?assertEqual("ok\n", Res),
    ok.
