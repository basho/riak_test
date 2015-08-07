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

-module(bdp_redis_start).
-behavior(riak_test).

-export([confirm/0]).

-spec confirm() -> 'pass' | 'fail'.
confirm() ->
    Cluster = build_cluster(3),
    [Node1 | _] = Cluster,
    ConfigArgs = ["r1", redis, [], false],
    lager:info("Adding and running redis on ~p", [Node1]),
    ok = rpc:call(Node1, data_platform_global_state, add_service_config, ConfigArgs),
    ok = rpc:call(Node1, data_platform_global_state, start_service, ["g1", "r1", Node1]),
    [ADevPath | _] = rtdev:devpaths(),
    RedisCli = ADevPath ++ "/dev/dev1/lib/data_platform/priv/redis/bin/redis-cli",
    Cmd = RedisCli ++ " ping",
    Got = os:cmd(Cmd),
    <<"PONG\n">> = Got.

%% copied from ensemble_util.erl
build_cluster(N) ->
    Nodes = rt:deploy_nodes(N),
    Node = hd(Nodes),
    rt:join_cluster(Nodes),
    ensemble_util:wait_until_cluster(Nodes),
    ensemble_util:wait_for_membership(Node),
    ensemble_util:wait_until_stable(Node, N),
    Nodes.
