%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(ts_cluster_capabilities_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() -> 
    rt:grep_test_functions(?MODULE).


%%--------------------------------------------------------------------
%% TESTS
%%--------------------------------------------------------------------

%% Start three nodes which are no clustered
%% With rpc, register capabilities
%%     one node has version 1
%%     two nodes have version 2
%% Join the cluster
%% Assert that all nodes return version 1 for the capability
cluster_capability_test(_Ctx) ->
    % [Node_A] = rt:start('dev1@127.0.0.1'),
    % [Node_B] = rt:start('dev2@127.0.0.1'),
    % [Node_C] = rt:start('dev3@127.0.0.1'),
    [Node_A, Node_B, Node_C] = rt:deploy_nodes(3),
    Cap_name = {rt, cap_1},
    ok = rpc:call(Node_A, riak_core_capability, register, [Cap_name, [2,1], 2]),
    ok = rpc:call(Node_B, riak_core_capability, register, [Cap_name, [1],   1]),
    ok = rpc:call(Node_C, riak_core_capability, register, [Cap_name, [2,1], 2]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ?assertEqual(1,rpc:call(Node_A, riak_core_capability, get, [Cap_name])),
    ?assertEqual(1,rpc:call(Node_B, riak_core_capability, get, [Cap_name])),
    ?assertEqual(1,rpc:call(Node_C, riak_core_capability, get, [Cap_name])),
    ok.
