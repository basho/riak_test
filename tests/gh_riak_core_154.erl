%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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

%% Automated test for issue riak_core#154
%% Hinted handoff does not occur after a node has been restarted in Riak 1.1
-module(gh_riak_core_154).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% Increase handoff concurrency on nodes
    NewConfig = [{riak_core, [{handoff_concurrency, 1024}]}],
    Nodes = rt_cluster:build_cluster(2, NewConfig),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    [Node1, Node2] = Nodes,

    lager:info("Write data while ~p is offline", [Node2]),
    rt_node:stop(Node2),
    rt:wait_until_unpingable(Node2),
    ?assertEqual([], rt_systest:write(Node1, 1000, 3)),

    lager:info("Verify that ~p is missing data", [Node2]),
    rt_node:start(Node2),
    rt_node:stop(Node1),
    rt:wait_until_unpingable(Node1),
    ?assertMatch([{_,{error,notfound}}|_],
                 rt_systest:read(Node2, 1000, 3)),

    lager:info("Restart ~p and wait for handoff to occur", [Node1]),
    rt_node:start(Node1),
    rt:wait_for_service(Node1, riak_kv),
    rt:wait_until_transfers_complete([Node1]),

    lager:info("Verify that ~p has all data", [Node2]),
    rt_node:stop(Node1),
    ?assertEqual([], rt_systest:read(Node2, 1000, 3)),

    lager:info("gh_riak_core_154: passed"),
    pass.
