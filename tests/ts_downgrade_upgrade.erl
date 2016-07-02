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

-define(CLUSTER_NODES, 3).

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
    Nodes = rt:build_cluster(
              lists:duplicate(?CLUSTER_NODES, OldVsn)),

    %% document the configuration of the nodes so that this can be added
    %% to the Config that is passed to all the tests
    NodeConfig = [
                  {nodes, lists:zip(lists:seq(1, ?CLUSTER_NODES), Nodes)},
                  {previous, OldVsn},
                  {current, NewVsn}
                 ],

    %% set up a separate, slave node for the 'previous' version
    %% client, to talk to downgraded nodes
    _ = application:start(crypto),
    Suffix = [crypto:rand_uniform($a, $z) || _ <- lists:seq(1,8)],
    PrevRiakcNode = list_to_atom("alsoran_"++Suffix++"@127.0.0.1"),
    ClientConfig = [
                    {previous_client_node, rt_client:set_up_slave_for_previous_client(PrevRiakcNode)}
                   ],
    %% ct:pal("~p", [code:which(rt_client)]),
    %% ct:pal("Client versions (current/previous): ~s/~s",
    %%        [rt_client:client_vsn(),
    %%         rpc:call(PrevRiakcNode, rt_client, client_vsn, [])]),  %% need to add -pa `pwd`/ebin to erl command for previous_client_node

    %% prepare the SELECT queries and the data these are expected to fetch
    QueryConfig = ts_updown_util:make_queries_and_data(),

    %% now stuff the config with the expected values
    FullConfig = QueryConfig ++ NodeConfig ++ ClientConfig ++ Config,
    %% ct:pal("CT config: ~p", [FullConfig]),
    FullConfig.

end_per_suite(Config) ->
    rt_slave:stop(
      proplists:get_value(previous_client_node, Config)),
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
      create_table_1u,
      create_table_2u,
      create_table_3u,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      downgrade3,
      %% at each iteration as we down- or upgrade a node, we create a
      %% distinctively named table, from which selects will be run.
      create_table_3d,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      downgrade2,
      create_table_2d,

      {group, query_group_1},
      {group, query_group_2},
      {group, query_group_3},
      {group, query_group_4},

      downgrade1,
      create_table_1d,

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

upgrade1(Config) -> ts_updown_util:do_node_transition(Config, 1, previous).
upgrade2(Config) -> ts_updown_util:do_node_transition(Config, 2, previous).
upgrade3(Config) -> ts_updown_util:do_node_transition(Config, 3, previous).

downgrade1(Config) -> ts_updown_util:do_node_transition(Config, 1, current).
downgrade2(Config) -> ts_updown_util:do_node_transition(Config, 2, current).
downgrade3(Config) -> ts_updown_util:do_node_transition(Config, 3, current).

create_table_1u(Config) -> ts_updown_util:create_versioned_table(Config, 1, current).
create_table_1d(Config) -> ts_updown_util:create_versioned_table(Config, 1, previous).
create_table_2u(Config) -> ts_updown_util:create_versioned_table(Config, 2, current).
create_table_2d(Config) -> ts_updown_util:create_versioned_table(Config, 2, previous).
create_table_3u(Config) -> ts_updown_util:create_versioned_table(Config, 3, current).
create_table_3d(Config) -> ts_updown_util:create_versioned_table(Config, 3, previous).

query_1(Config)  -> ts_updown_util:run_queries(Config, 1).
query_2(Config)  -> ts_updown_util:run_queries(Config, 2).
query_3(Config)  -> ts_updown_util:run_queries(Config, 3).
query_4(Config)  -> ts_updown_util:run_queries(Config, 4).
query_5(Config)  -> ts_updown_util:run_queries(Config, 5).
query_6(Config)  -> ts_updown_util:run_queries(Config, 6).
query_7(Config)  -> ts_updown_util:run_queries(Config, 7).
query_8(Config)  -> ts_updown_util:run_queries(Config, 8).
query_9(Config)  -> ts_updown_util:run_queries(Config, 9).
query_10(Config) -> ts_updown_util:run_queries(Config, 10).
