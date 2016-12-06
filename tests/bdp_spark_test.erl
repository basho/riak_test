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

-module(bdp_spark_test).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%spark master definitions
-define(SPARK_MASTER_SERVICE_NAME, "my-spark-master").
-define(SPARK_MASTER_SERVICE_TYPE, "spark-master").
-define(SPARK_MASTER_SERVICE_CONFIG, [{"HOST", "127.0.0.1"},{"RIAK_HOST","127.0.0.1:8087"}]).
%spark worker definitions
-define(SPARK_WORKER_SERVICE_NAME, "my-spark-worker").
-define(SPARK_WORKER_SERVICE_TYPE, "spark-worker").
-define(SPARK_WORKER_SERVICE_CONFIG, [{"MASTER_URL", "spark://127.0.0.1:7077"},{"SPARK_WORKER_PORT","8081"}]).


%this code tests if a spark job can successfully be submitted to a two node spark cluster within a three node bdp cluster

%WARNINGS: spark worker configs must contain exact ip address of spark master, not 127.0.0.1.  Otherwise, spark master and worker
%will fail to link together.  As such, os:cmd is used to get the ip address of node1 via a shell command that might not work on systems other than moc osx.
confirm() ->

    %build cluster
    lager:info("Building cluster"),
    ClusterSize = 3,
    ClusterConfig = [{lager, [{handlers, [{file, "console.log"}, {level, debug}] }]}],
    Nodes = [Node1, Node2, _Node3] = rt:deploy_nodes(ClusterSize, ClusterConfig),
    rt:join_cluster(Nodes),
    %let cluster settle down
    lager:info("Waiting 20 seconds..."),
    timer:sleep(20000),
   
    %% add and start master service to node1
    ok = bdp_util:service_added(Node1, ?SPARK_MASTER_SERVICE_NAME, ?SPARK_MASTER_SERVICE_TYPE, ?SPARK_MASTER_SERVICE_CONFIG),
    lager:info("Service ~p (~s) added", [?SPARK_MASTER_SERVICE_NAME, ?SPARK_MASTER_SERVICE_TYPE]),
    lager:info("Waiting 10 seconds..."),
    timer:sleep(10000),

    ok = bdp_util:service_started(Node1, Node1, ?SPARK_MASTER_SERVICE_NAME, ?SPARK_MASTER_SERVICE_TYPE),
    lager:info("Service ~p up   on ~p", [?SPARK_MASTER_SERVICE_NAME, Node1]),
    lager:info("Waiting 10 seconds..."),
    timer:sleep(10000),

    %% get master ip for worker configs
    %%be careful
    SparkMasterIP = re:replace(rpc:call(Node1,os,cmd,["ipconfig getifaddr en0"]),"\\s+", "", [global,{return,list}]),
    SparkMasterAddr = "spark://"++SparkMasterIP++":7077",
    WorkerConfig = [{"MASTER_URL", SparkMasterAddr},{"SPARK_WORKER_PORT","8081"}],

    %%add and start worker service
    ok = bdp_util:service_added(Node2, ?SPARK_WORKER_SERVICE_NAME, ?SPARK_WORKER_SERVICE_TYPE, WorkerConfig),
    lager:info("Service ~p (~s) added", [?SPARK_WORKER_SERVICE_NAME, ?SPARK_WORKER_SERVICE_TYPE]),
    lager:info("Waiting 10 seconds..."),
    timer:sleep(10000),

    ok = bdp_util:service_started(Node2, Node2, ?SPARK_WORKER_SERVICE_NAME, ?SPARK_WORKER_SERVICE_TYPE),
    lager:info("Service ~p up   on ~p", [?SPARK_WORKER_SERVICE_NAME, Node2]),
    %wait for master and worker to properly communicate (recommended 20+ seconds from expirement)
    lager:info("Waiting 30 seconds..."),
    timer:sleep(30000),

    %%run spark job pi.py
    SparkJobSubmit1 = "./lib/data_platform-1/priv/spark-master/bin/spark-submit --master ",
    SparkJobSubmit2 = " ./lib/data_platform-1/priv/spark-master/examples/src/main/python/pi.py 100",
    SparkJobSubmit3 = SparkJobSubmit1++SparkMasterAddr++SparkJobSubmit2,
    Results = rpc:call(Node2,os,cmd,[SparkJobSubmit3]),
    lager:info("Spark Job Results: ~s", [Results]),

    %Test if the spark job submission worked
    ?assert(string:str(Results,"Pi is roughly") > 0),

    pass.


