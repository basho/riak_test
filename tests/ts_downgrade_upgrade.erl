%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(ts_downgrade_upgrade).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").


%% Callbacks

suite() ->
    [{timetrap,{seconds,9000}}].

init_per_suite(Config) ->
    lager:info("****************************************~n", []),

    %% get the test meta data from the riak_test runner
    TestMetaData = riak_test_runner:metadata(self()),

    %% set up the cluster that we will be testing
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, current),
    NewVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    %% build the starting (old cluster)
    Nodes = rt:build_cluster([OldVsn, OldVsn, OldVsn, OldVsn, OldVsn]),
    
    %% document the configuration of the nodes so that this can be added
    %% to the Config that is passed to all the tests
    NodeConfig = [
                  {nodes, lists:zip(lists:seq(1,5), Nodes)},
                  {oldvsn, OldVsn}, 
                  {newvsn, NewVsn}
                 ],

    %% now we are going to write some data to the old cluster
    %% and generate some queries that will operate on it
    %% the query and the expected results will be put in the Config
    %% so that we can rerun them as we walk the upgrade/downgrade ladder
    %% Gonnae do a complex aggregation query and a simple read for functional
    %% coverage
    QueryConfig = ts_updown_util:init_per_suite_data_write(Nodes),
    %% now stuff the config with the expected values
    QueryConfig ++ NodeConfig ++ Config.

end_per_suite(_Config) ->
    lager:info("in end_per_suite", []),
    lager:info("****************************************", []),
    ok.

init_per_group(_GroupName, Config) ->
    lager:info("in init_per_group", []),
    Config.

end_per_group(_GroupName, _Config) ->
    lager:info("in end_per_group", []),
    ok.

init_per_testcase(_TestCase, Config) ->
    lager:info("in init_per_testcase", []),
    Config.

end_per_testcase(_TestCase, _Config) ->
    lager:info("in end_per_testcase", []),
    ok.

%% we need to break up the read tests into groups to stop the system going into
%% query overload
groups() ->
    [
     {can_still_read_tests_1, [parallel], [
                                           can_still_read_query1,
                                           can_still_read_query2,
                                           can_still_read_query3
                                          ]},
     {can_still_read_tests_2, [parallel], [
                                           can_still_read_query4,
                                           can_still_read_query5,
                                           can_still_read_query6
                                          ]},
     {can_still_read_tests_3, [parallel], [
                                           can_still_read_query7,
                                           can_still_read_query8,
                                           can_still_read_query9
                                          ]},
     {can_still_read_tests_4, [parallel], [
                                           can_still_read_query10
                                          ]}
    ].

 all() -> 
     [
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      downgrade5,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      downgrade4,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      downgrade3,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      downgrade2,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      downgrade1,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      upgrade1,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      upgrade2,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      upgrade3,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      upgrade4,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4},
      upgrade5,
      {group, can_still_read_tests_1},
      {group, can_still_read_tests_2},
      {group, can_still_read_tests_3},
      {group, can_still_read_tests_4}
     ].

%%%
%%% Tests
%%

upgrade1(Config) -> ts_updown_util:do_node_transition(Config, 1, newvsn).
upgrade2(Config) -> ts_updown_util:do_node_transition(Config, 2, newvsn).
upgrade3(Config) -> ts_updown_util:do_node_transition(Config, 3, newvsn).
upgrade4(Config) -> ts_updown_util:do_node_transition(Config, 4, newvsn).
upgrade5(Config) -> ts_updown_util:do_node_transition(Config, 5, newvsn).

downgrade1(Config) -> ts_updown_util:do_node_transition(Config, 1, oldvsn).
downgrade2(Config) -> ts_updown_util:do_node_transition(Config, 2, oldvsn).
downgrade3(Config) -> ts_updown_util:do_node_transition(Config, 3, oldvsn).
downgrade4(Config) -> ts_updown_util:do_node_transition(Config, 4, oldvsn).
downgrade5(Config) -> ts_updown_util:do_node_transition(Config, 5, oldvsn).

can_still_read_query1()  -> ts_updown_util:run_init_per_suite_queries(1).
can_still_read_query2()  -> ts_updown_util:run_init_per_suite_queries(2).
can_still_read_query3()  -> ts_updown_util:run_init_per_suite_queries(3).
can_still_read_query4()  -> ts_updown_util:run_init_per_suite_queries(4).
can_still_read_query5()  -> ts_updown_util:run_init_per_suite_queries(5).
can_still_read_query6()  -> ts_updown_util:run_init_per_suite_queries(6).
can_still_read_query7()  -> ts_updown_util:run_init_per_suite_queries(7).
can_still_read_query8()  -> ts_updown_util:run_init_per_suite_queries(8).
can_still_read_query9()  -> ts_updown_util:run_init_per_suite_queries(9).
can_still_read_query10() -> ts_updown_util:run_init_per_suite_queries(10).

can_still_read_query1(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 1).
can_still_read_query2(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 2).
can_still_read_query3(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 3).
can_still_read_query4(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 4).
can_still_read_query5(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 5).
can_still_read_query6(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 6).
can_still_read_query7(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 7).
can_still_read_query8(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 8).
can_still_read_query9(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 9).
can_still_read_query10(Config) -> ts_updown_util:run_init_per_suite_queries(Config, 10).
