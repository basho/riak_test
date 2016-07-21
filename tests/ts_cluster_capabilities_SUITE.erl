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
    %% tear down the whole cluster before every test
    rtdev:setup_harness('_', '_'),
    ct:pal("TEST CASE ~p", [_TestCase]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() -> 
    rt:grep_test_functions(?MODULE).

-define(TS_VERSION_CURRENT, "current").
-define(TS_VERSION_1_3, "ts_1.3.1").

-define(SQL_SELECT_CAP, {riak_kv, sql_select_version}).

%%--------------------------------------------------------------------
%% Basic Capability System Tests
%%--------------------------------------------------------------------

%% Start three nodes which are no clustered
%% With rpc, register capabilities
%%     one node has version 1
%%     two nodes have version 2
%% Join the cluster
%% Assert that all nodes return version 1 for the capability
capabilities_are_mixed_test(_Ctx) ->
    [Node_A, Node_B, Node_C] = rt:deploy_nodes(3),
    Cap_name = {rt, cap_1},
    V1 = 1,
    V2 = 2,
    ok = rpc:call(Node_A, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]),
    ok = rpc:call(Node_B, riak_core_capability, register, [Cap_name, [V1],    V1, V1]),
    ok = rpc:call(Node_C, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    rt:wait_until_capability(Node_A, Cap_name, V1),
    rt:wait_until_capability(Node_B, Cap_name, V1),
    rt:wait_until_capability(Node_C, Cap_name, V1),
    ok.

capabilities_are_same_on_all_nodes_test(_Ctx) ->
    [Node_A, Node_B, Node_C] = rt:deploy_nodes(3),
    Cap_name = {rt, cap_2},
    V1 = 1,
    V2 = 2,
    ok = rpc:call(Node_A, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]), %% if the preference is [1,2] then the cap 
    ok = rpc:call(Node_B, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]), %% value will be 1 for all nodes
    ok = rpc:call(Node_C, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    rt:wait_until_capability(Node_A, Cap_name, V2),
    rt:wait_until_capability(Node_B, Cap_name, V2),
    rt:wait_until_capability(Node_C, Cap_name, V2),
    ok.

other_nodes_do_not_have_capability_test(_Ctx) ->
    [Node_A, Node_B, Node_C] = rt:deploy_nodes(3),
    Cap_name = {rt, cap_3},
    V1 = 1,
    V2 = 2,
    ok = rpc:call(Node_B, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]), %% value will be 1 for all nodes
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    rt:wait_until_capability(Node_B, Cap_name, V1),
    ok.

capability_not_specified_on_one_node_test(_Ctx) ->
    [Node_A, Node_B, Node_C] = rt:deploy_nodes(3),
    Cap_name = {rt, cap_4},
    V1 = 1,
    V2 = 2,
    ok = rpc:call(Node_A, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]), %% if the preference is [1,2] then the cap 
    ok = rpc:call(Node_B, riak_core_capability, register, [Cap_name, [V2,V1], V1, V1]), %% value will be 1 for all nodes
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    rt:wait_until_capability(Node_A, Cap_name, V1),
    rt:wait_until_capability(Node_B, Cap_name, V1),
    ok.

%%--------------------------------------------------------------------
%% Riak TS Capability Tests
%%--------------------------------------------------------------------

sql_select_upgrade_a_node_from_1_3_test(_) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_1_3, ?TS_VERSION_1_3, ?TS_VERSION_1_3]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ok = rt:upgrade(Node_A, ?TS_VERSION_CURRENT),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_capability(Node_A, ?SQL_SELECT_CAP, 1),
    ok.

sql_select_join_with_all_nodes_upgraded_test(_) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_1_3, ?TS_VERSION_1_3, ?TS_VERSION_1_3]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    rt:upgrade(Node_A, ?TS_VERSION_CURRENT),
    rt:upgrade(Node_B, ?TS_VERSION_CURRENT),
    rt:upgrade(Node_C, ?TS_VERSION_CURRENT),
    rt:wait_until_capability(Node_A, ?SQL_SELECT_CAP, 2),
    rt:wait_until_capability(Node_B, ?SQL_SELECT_CAP, 2),
    rt:wait_until_capability(Node_C, ?SQL_SELECT_CAP, 2),
    ok.

sql_select_downgrade_a_node_test(_) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    rt:wait_until_capability(Node_A, ?SQL_SELECT_CAP, 2),
    rt:wait_until_capability(Node_B, ?SQL_SELECT_CAP, 2),
    rt:wait_until_capability(Node_C, ?SQL_SELECT_CAP, 2),
    rt:upgrade(Node_A, ?TS_VERSION_1_3),
    rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    rt:wait_until_capability(Node_B, ?SQL_SELECT_CAP, 1),
    rt:wait_until_capability(Node_C, ?SQL_SELECT_CAP, 1),
    ok.

