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
-module(ts_updown_select_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Cluster = ts_util:cluster_and_connect(multiple),
    [{cluster, Cluster} | Config].

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
    [simple_query].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

simple_query() -> 
    [].

simple_query(Config) -> 
    {cluster, Cluster} = lists:keyfind(cluster, 1, Config),
    TestType = normal,
    DDL = ts_util:get_ddl(),
    Data = ts_util:get_valid_select_data(),
    Qry = ts_util:get_valid_qry(),
    Expected =
        {ts_util:get_cols(small),
         ts_util:exclusive_result_from_data(Data, 2, 9)},
    Got = ts_util:ts_query(Cluster, TestType, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    ok.
