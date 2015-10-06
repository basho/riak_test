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

-module(bdp_spark_pi_submit_test).
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


% this code tests if a spark job can successfully be submitted to a two
% node spark cluster within a three node bdp cluster

% WARNINGS: spark worker configs must contain exact ip address of spark
% master, not 127.0.0.1.  Otherwise, spark master and worker will fail
% to link together.  As such, os:cmd is used to get the ip address of
% node1 via a shell command that might not work on systems other than
% moc osx.

confirm() ->

    %build cluster
    lager:info("Building cluster"),
    ClusterSize = 3,
    _Nodes = [Node1, Node2, _Node3] =
        bdp_util:build_cluster(
          ClusterSize, [{lager, [{handlers, [{file, "console.log"}, {level, debug}] }]}]),

    %% add spark master and worker services
    lager:info("Adding Spaker Master and Spark Worker Service..."),
    %% add master service
    ok = bdp_util:add_service(Node1, ?SPARK_MASTER_SERVICE_NAME, ?SPARK_MASTER_SERVICE_TYPE, ?SPARK_MASTER_SERVICE_CONFIG),
    ok = bdp_util:wait_services(Node1, {[], [?SPARK_MASTER_SERVICE_NAME]}),
    %% add worker service
    SparkMasterIP = re:replace(rpc:call(Node1,os,cmd,["ipconfig getifaddr en0"]),"\\s+", "", [global,{return,list}]),
    SparkMasterAddr = "spark://"++SparkMasterIP++":7077",
    WorkerConfig = [{"MASTER_URL", SparkMasterAddr},{"SPARK_WORKER_PORT","8081"}],

    ok = bdp_util:add_service(Node2, ?SPARK_WORKER_SERVICE_NAME, ?SPARK_WORKER_SERVICE_TYPE, WorkerConfig),
    ok = bdp_util:wait_services(Node2, {[], [?SPARK_MASTER_SERVICE_NAME, ?SPARK_WORKER_SERVICE_NAME]}),

    lager:info("Service ~p (~s) added", [?SPARK_MASTER_SERVICE_NAME, ?SPARK_MASTER_SERVICE_TYPE]),
    %start master and worker
    ok = bdp_util:start_seervice(Node1, Node1, ?SPARK_MASTER_SERVICE_NAME, ?SPARK_MASTER_SERVICE_TYPE),
    ok = bdp_util:wait_services(Node1, {[?SPARK_MASTER_SERVICE_NAME], [?SPARK_MASTER_SERVICE_NAME, ?SPARK_WORKER_SERVICE_NAME]}),
    ok = bdp_util:start_seervice(Node2, Node2, ?SPARK_WORKER_SERVICE_NAME, ?SPARK_MASTER_SERVICE_TYPE),
    ok = bdp_util:wait_services(Node2, {[?SPARK_MASTER_SERVICE_NAME,?SPARK_WORKER_SERVICE_NAME], [?SPARK_MASTER_SERVICE_NAME, ?SPARK_WORKER_SERVICE_NAME]}),

    lager:info("Waiting 10 seconds..."),
    timer:sleep(10000),

    %check running services
    {Run,Ava} = bdp_util:get_services(Node1),

    MyServices = ["my-spark-master","my-spark-worker"],
    ?assert(Run == MyServices),
    ?assert(Ava == MyServices),
    lager:info("~s",[MyServices]),
    lager:info("Running: ~s", [Run]),
    lager:info("Available: ~s", [Ava]),

    %%run spark job pi.py
    SparkJobSubmit1 = "./lib/data_platform-1/priv/spark-master/bin/spark-submit --master ",
    SparkJobSubmit2 = " ./lib/data_platform-1/priv/spark-master/examples/src/main/python/pi.py 100",
    SparkJobSubmit3 = SparkJobSubmit1++SparkMasterAddr++SparkJobSubmit2,
    Results = rpc:call(Node2,os,cmd,[SparkJobSubmit3]),
    lager:info("Spark Job Results: ~s", [Results]),

    %Test if the spark job submission worked
    ?assert(string:str(Results,"Pi is roughly") > 0),

    %Assert proper cluster execution of spark job
    WgetCall = "wget "++SparkMasterIP++":8080",
    lager:info("wget call: ~s",[WgetCall]),
    rpc:call(Node1,os,cmd,[WgetCall]),
    {ok, File} = rpc:call(Node1,file,read_file,["index.html"]),
    Content = unicode:characters_to_list(File),
    %assert master and worker see each other
    TestString1 = "<li><strong>Workers:</strong> 1</li>",
    ?assert(string:str(Content,TestString1) > 0),
    %assert job execution in cluster
    TestString2 = "<li><strong>Applications:</strong>
                0 Running,
                1 Completed </li>",
    ?assert(string:str(Content,TestString2) > 0),

    pass.


