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

-module(bdp_redis).
-behavior(riak_test).

-export([confirm/0]).

-spec confirm() -> 'pass' | 'fail'.
confirm() ->
    Cluster = bdp_util:build_cluster(3),
    [Node1 | _] = Cluster,
    [DevPath | _] = rtdev:devpaths(),
    RedisCliPath = DevPath ++ "/dev/dev1/lib/data_platform/priv/redis/bin/redis-cli",

    ok = redis_start_test(Node1, RedisCliPath),

    ok = redis_crash_test(Node1, RedisCliPath),

    ok = redis_stop_test(Node1, RedisCliPath),

    pass.

redis_start_test(Node, RedisCli) ->
    lager:info("=== START REDIS ==="),
    ok = bdp_util:add_service(Node, "r1", "redis", []),
    ok = bdp_util:start_service(Node, Node, "r1", "g1"),

    ok = rt:wait_until(Node, fun(_N) ->
        case rpc:call(Node, data_platform_service_sup, services, []) of
            [_ProllyRedis] ->
                true;
            _ ->
                false
        end
    end),

    PingCmd = RedisCli ++ " ping",
    Pong = os:cmd(PingCmd),
    "PONG\n" = Pong,
    ok.

redis_crash_test(Node, RedisCli) ->
    lager:info("=== CRASH REDIS ==="),
    Services = rpc:call(Node, data_platform_service_sup, services, []),
    Name = {"g1", "r1", Node},
    {Name, Pid} = lists:keyfind(Name, 1, Services),
    OsPid = rpc:call(Node, data_platform_service, os_pid, [Pid]),
    [] = os:cmd("kill -9 " ++ integer_to_list(OsPid)),
    PingCmd = RedisCli ++ " ping",
    rt:wait_until(fun() ->
        "PONG\n" =:= os:cmd(PingCmd)
    end).

redis_stop_test(Node, RedisCli) ->
    lager:info("=== STOP REDIS ==="),
    ok = bdp_util:stop_service(Node, Node, "r1", "g1"),
    PingCmd = RedisCli ++ " ping",
    rt:wait_until(fun() ->
        "PONG\n" =/= os:cmd(PingCmd)
    end).