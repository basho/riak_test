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
-module(ts_downgrade_upgrade).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").


%% Callbacks

suite() ->
    [{timetrap,{seconds,9000}}].

init_per_suite(Config) ->
    %% get the test meta data from the riak_test runner
    TestMetaData = riak_test_runner:metadata(self()),

    %% set up the cluster that we will be testing
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, current),
    NewVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    %% build the starting (old cluster)
    Nodes = rt:build_cluster([OldVsn, OldVsn, OldVsn]),

    %% document the configuration of the nodes so that this can be added
    %% to the Config that is passed to all the tests
    NodeConfig = [
                  {nodes, lists:zip(lists:seq(1,3), Nodes)},
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
    FullConfig = QueryConfig ++ NodeConfig ++ Config,
    %% ct:pal("CT config: ~p", [FullConfig]),
    FullConfig.

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

%% we need to break up the read tests into groups to stop the system going into
%% query overload
groups() ->
    [
     {query_group_1, [parallel], [
                                  query_1,
                                  query_2,
                                  query_3
                                 ]},
     {query_group_2, [parallel], [
                                  query_4,
                                  query_5,
                                  query_6
                                 ]},
     {query_group_3, [parallel], [
                                  query_7,
                                  query_8,
                                  query_9
                                 ]},
     {query_group_4, [parallel], [
                                  query_10
                                 ]}
    ].

 all() ->
     [
      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      downgrade3,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      downgrade2,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      downgrade1,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      upgrade1,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      upgrade2,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      upgrade3,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4}
     ].

%%%
%%% Tests
%%

upgrade1(Config) -> ts_updown_util:do_node_transition(Config, 1, oldvsn).
upgrade2(Config) -> ts_updown_util:do_node_transition(Config, 2, oldvsn).
upgrade3(Config) -> ts_updown_util:do_node_transition(Config, 3, oldvsn).

downgrade1(Config) -> ts_updown_util:do_node_transition(Config, 1, newvsn).
downgrade2(Config) -> ts_updown_util:do_node_transition(Config, 2, newvsn).
downgrade3(Config) -> ts_updown_util:do_node_transition(Config, 3, newvsn).

query_1()  -> ts_updown_util:run_init_per_suite_queries(1).
query_2()  -> ts_updown_util:run_init_per_suite_queries(2).
query_3()  -> ts_updown_util:run_init_per_suite_queries(3).
query_4()  -> ts_updown_util:run_init_per_suite_queries(4).
query_5()  -> ts_updown_util:run_init_per_suite_queries(5).
query_6()  -> ts_updown_util:run_init_per_suite_queries(6).
query_7()  -> ts_updown_util:run_init_per_suite_queries(7).
query_8()  -> ts_updown_util:run_init_per_suite_queries(8).
query_9()  -> ts_updown_util:run_init_per_suite_queries(9).
query_10() -> ts_updown_util:run_init_per_suite_queries(10).

query_1(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 1).
query_2(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 2).
query_3(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 3).
query_4(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 4).
query_5(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 5).
query_6(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 6).
query_7(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 7).
query_8(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 8).
query_9(Config)  -> ts_updown_util:run_init_per_suite_queries(Config, 9).
query_10(Config) -> ts_updown_util:run_init_per_suite_queries(Config, 10).
