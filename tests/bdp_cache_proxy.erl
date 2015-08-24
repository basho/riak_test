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

-module(bdp_cache_proxy).
-behavior(riak_test).

-export([confirm/0]).

-spec confirm() -> 'pass' | 'fail'.
confirm() ->
    Cluster = bdp_util:build_cluster(3),
    [Node1 | _] = Cluster,

    % proxy depends on redis
    ok = bdp_util:add_service(Node1, "r1", "redis", []),
    ok = bdp_util:start_service(Node1, Node1, "r1", "g1"),

    [DevPath | _] = rtdev:devpaths(),
    ProxyPath = DevPath ++ "/dev/dev1/lib/data_platform/priv/proxy/bin/nutcracker",

    ok = proxy_start_test(Node1, ProxyPath),

    ok = proxy_crash_test(Node1, ProxyPath),

    ok = proxy_stop_test(Node1, ProxyPath),

    pass.

proxy_start_test(Node, _Path) ->
    lager:info("=== START PROXY ==="),

    ok = bdp_util:add_service(Node, "p1", "cache-proxy", []),
    ok = bdp_util:start_service(Node, Node, "p1", "g1"),
    
    rt:wait_until(fun() ->
        case get_stats() of
            {struct, _} ->
                true;
            Blorp ->
                Blorp
        end
    end).

proxy_crash_test(Node, _Path) ->
    lager:info("=== CRASH PROXY ==="),

    Services = rpc:call(Node, data_platform_service_sup, services, []),
    Name = {"g1", "p1", Node},
    {Name, Pid} = lists:keyfind(Name, 1, Services),
    OsPid = rpc:call(Node, data_platform_service, os_pid, [Pid]),
    [] = os:cmd("kill -9 " ++ integer_to_list(OsPid)),

    rt:wait_until(fun() ->
        case get_stats() of
            {struct, _} ->
                true;
            Else ->
                Else
        end
    end).

proxy_stop_test(Node, _Path) ->
    lager:info("=== STOP PROXY ==="),

    ok = bdp_util:stop_service(Node, Node, "p1", "g1"),

    rt:wait_until(fun() ->
        {error, econnrefused} =:= get_stats()
    end).

get_stats() ->
    case gen_tcp:connect("127.0.0.1", 22123, [{active, false}, binary]) of
        {ok, Socket} ->
            get_stats(Socket, <<>>);
        Else ->
            Else
    end.

get_stats(Socket, Acc) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Bin} ->
            get_stats(Socket, <<Bin/binary, Acc/binary>>);
        {error, closed} ->
            mochijson2:decode(Acc);
        Wut ->
            lager:info("wut: ~p", [Wut]),
            Wut
    end.
