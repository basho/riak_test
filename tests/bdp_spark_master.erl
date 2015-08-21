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

-module(bdp_spark_master).
-behavior(riak_test).

-export([confirm/0]).

-spec confirm() -> 'pass' | 'fail'.
confirm() ->
    Cluster = bdp_util:build_cluster(3),
    [Node1 | _] = Cluster,

    [DevPath | _] = rtdev:devpaths(),
    SparkPath = DevPath ++ "/dev/dev1/lib/data_platform/priv/spark",

    application:start(inets),

    ok = spark_master_start_test(Node1, SparkPath),

    ok = spark_master_crash_test(Node1, SparkPath),

    ok = spark_master_stop_test(Node1, SparkPath),

    pass.

spark_master_start_test(Node, _Path) ->
    lager:info("=== Start Spark Master ==="),

    ok = bdp_util:add_service(Node, "sm1", "spark-master", []),
    ok = bdp_util:start_service(Node, Node, "sm1", "g1"),

    % if spark is doing a background run, this will never finish waiting
    % since the supervisor will never add the dying child to it's specs.
    ok = rt:wait_until(Node, fun(_N) ->
        case rpc:call(Node, data_platform_service_sup, services, []) of
            [_ProllySpark] ->
                true;
            _ ->
                false
        end
    end),

    % let's see if spark really is running.
    rt:wait_until(fun() ->
        case httpc:request("http://localhost:8080") of
            {ok, _Text} ->
                true;
            _ ->
                false
        end
    end).

spark_master_crash_test(Node, _Path) ->
    lager:info("=== Crash Spark Master ==="),

    Services = rpc:call(Node, data_platform_service_sup, services, []),

    Name = {"g1", "sm1", Node},
    {Name, Pid} = lists:keyfind(Name, 1, Services),
    OsPid = rpc:call(Node, data_platform_service, os_pid, [Pid]),
    ErlPid = rpc:call(Node, data_platform_service, erl_pid, [Pid]),
    [] = os:cmd("kill -9 " ++ integer_to_list(OsPid)),
    Mon = erlang:monitor(process, ErlPid),
    ok = receive
        {'DOWN', Mon, process, ErlPid, _} ->
            ok
    end,
    rt:wait_until(fun() ->
        case httpc:request("http://localhost:8080") of
            {ok, _Data} ->
                true;
            _ ->
                false
        end
    end).

spark_master_stop_test(Node, _Path) ->
    lager:info("=== Stop Spark Master ==="),

    Services = rpc:call(Node, data_platform_service_sup, services, []),
    Name = {"g1", "sm1", Node},
    {Name, Pid} = lists:keyfind(Name, 1, Services),
    ErlPid = rpc:call(Node, data_platform_service, erl_pid, [Pid]),

    ok = bdp_util:stop_service(Node, Node, "sm1", "g1"),

    Mon = erlang:monitor(process, ErlPid),
    ok = receive
        {'DOWN', Mon, process, ErlPid, _} ->
            ok
    end,
    {error, econrefused} = httpc:request("http://localhost:8080"),
    ok.
