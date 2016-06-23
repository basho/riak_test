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
-module(ts_updown_util).

-include_lib("eunit/include/eunit.hrl").

-export([
         init_per_suite_data_write/1,
         do_node_transition/3,
         run_init_per_suite_queries/2
        ]).

-define(TABLE, "Aggregation_written_on_old_cluster").
-define(TEMPERATURE_COL_INDEX, 4).
-define(PRESSURE_COL_INDEX, 5).
-define(PRECIPITATION_COL_INDEX, 6).

-define(CFG(K, C), proplists:get_value(K, C)).

init_per_suite_data_write(Nodes) ->
    DDL = ts_util:get_ddl(aggregation, ?TABLE),
    Conn = rt:pbc(hd(Nodes)),
    {ok, _} = riakc_ts:query(Conn, DDL),

    %% generate data and hoy it into the cluster
    Count = 10,
    Data = ts_util:get_valid_aggregation_data(Count),
    Column4 = [element(?TEMPERATURE_COL_INDEX,   X) || X <- Data],
    Column5 = [element(?PRESSURE_COL_INDEX,      X) || X <- Data],
    Column6 = [element(?PRECIPITATION_COL_INDEX, X) || X <- Data],
    ok = riakc_ts:put(Conn, ?TABLE, Data),

    %% now lets create some queries with their expected results
    Where = " WHERE myfamily = 'family1' and myseries = 'seriesX' "
        "and time >= 1 and time <= " ++ integer_to_list(Count),

    Qry1 = "SELECT COUNT(myseries) FROM " ++ ?TABLE ++ Where,
    Expected1 = {[<<"COUNT(myseries)">>], [{Count}]},

    Qry2 = "SELECT COUNT(time) FROM " ++ ?TABLE ++ Where,
    Expected2 = {[<<"COUNT(time)">>], [{Count}]},

    Qry3 = "SELECT COUNT(pressure), count(temperature), cOuNt(precipitation) FROM " ++
        ?TABLE ++ Where,
    Expected3 = {
      [<<"COUNT(pressure)">>,
       <<"COUNT(temperature)">>,
       <<"COUNT(precipitation)">>
      ],
      [{count_non_nulls(Column5),
        count_non_nulls(Column4),
        count_non_nulls(Column6)}]},

    Qry4 = "SELECT SUM(temperature) FROM " ++ ?TABLE ++ Where,
    Sum4 = lists:sum([X || X <- Column4, is_number(X)]),
    Expected4 = {[<<"SUM(temperature)">>],
                 [{Sum4}]},

    Qry5 = "SELECT SUM(temperature), sum(pressure), sUM(precipitation) FROM " ++
        ?TABLE ++ Where,
    Sum5 = lists:sum([X || X <- Column5, is_number(X)]),
    Sum6 = lists:sum([X || X <- Column6, is_number(X)]),
    Expected5 = {[<<"SUM(temperature)">>, <<"SUM(pressure)">>, <<"SUM(precipitation)">>],
                 [{Sum4, Sum5, Sum6}]},

    Qry6 = "SELECT MIN(temperature), MIN(pressure) FROM " ++ ?TABLE ++ Where,
    Min4 = lists:min([X || X <- Column4, is_number(X)]),
    Min5 = lists:min([X || X <- Column5, is_number(X)]),
    Expected6 = {[<<"MIN(temperature)">>, <<"MIN(pressure)">>],
                 [{Min4, Min5}]},

    Qry7 = "SELECT MAX(temperature), MAX(pressure) FROM " ++ ?TABLE ++ Where,
    Max4 = lists:max([X || X <- Column4, is_number(X)]),
    Max5 = lists:max([X || X <- Column5, is_number(X)]),
    Expected7 = {[<<"MAX(temperature)">>, <<"MAX(pressure)">>],
                 [{Max4, Max5}]},

    C4 = [X || X <- Column4, is_number(X)],
    C5 = [X || X <- Column5, is_number(X)],
    Count4 = length(C4),
    Count5 = length(C5),

    Avg4 = Sum4 / Count4,
    Avg5 = Sum5 / Count5,
    Qry8 = "SELECT AVG(temperature), MEAN(pressure) FROM " ++ ?TABLE ++ Where,
    Expected8 = {[<<"AVG(temperature)">>, <<"MEAN(pressure)">>],
                 [{Avg4, Avg5}]},

    StdDevFun4 = stddev_fun_builder(Avg4),
    StdDevFun5 = stddev_fun_builder(Avg5),
    StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / Count4),
    StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / Count5),
    Sample4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / (Count4-1)),
    Sample5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / (Count5-1)),
    Qry9 = "SELECT STDDEV_POP(temperature), STDDEV_POP(pressure),"
           " STDDEV(temperature), STDDEV(pressure), "
           " STDDEV_SAMP(temperature), STDDEV_SAMP(pressure) FROM " ++ ?TABLE ++ Where,
    Expected9 = {
      [
       <<"STDDEV_POP(temperature)">>, <<"STDDEV_POP(pressure)">>,
       <<"STDDEV(temperature)">>, <<"STDDEV(pressure)">>,
       <<"STDDEV_SAMP(temperature)">>, <<"STDDEV_SAMP(pressure)">>
      ],
      [{StdDev4, StdDev5, Sample4, Sample5, Sample4, Sample5}]
     },

    Qry10 = "SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM " ++
        ?TABLE ++ Where,
    Expected10 = {
      [<<"SUM(temperature)">>, <<"MIN(pressure)">>, <<"AVG(pressure)">>],
      [{Sum4, Min5, Avg5}]
     },

    [
     {init_per_suite_queries,
      [
       {1,  {Qry1,  {ok, Expected1}}},
       {2,  {Qry2,  {ok, Expected2}}},
       {3,  {Qry3,  {ok, Expected3}}},
       {4,  {Qry4,  {ok, Expected4}}},
       {5,  {Qry5,  {ok, Expected5}}},
       {6,  {Qry6,  {ok, Expected6}}},
       {7,  {Qry7,  {ok, Expected7}}},
       {8,  {Qry8,  {ok, Expected8}}},
       {9,  {Qry9,  {ok, Expected9}}},
       {10, {Qry10, {ok, Expected10}}}
      ]
     }
    ].


do_node_transition(Config, N, Version) ->
    Nodes = ?CFG(nodes, Config),
    Node  = ?CFG(N, Nodes),
    ToVsn = ?CFG(Version, Config),
    ok = rt:upgrade(Node, ToVsn),
    ok = rt:wait_for_service(Node, riak_kv),
    pass.


run_init_per_suite_queries(Config, TestNo) ->
    Queries = ?CFG(init_per_suite_queries, Config),
    {Query, Exp} = ?CFG(TestNo, Queries),
    Nodes = ?CFG(nodes, Config),

    %% try reading data from all nodes: we will thus cover both reading
    %%
    %% (a) from upgraded and downgraded nodes when other nodes are,
    %%     similarly, upgraded or downgraded;
    %%
    %% (b) from both the node the initial writing was done at, as well
    %%     as from other nodes.
    Success =
        lists:all(
          fun({NodeNo, Node}) ->
                  Got = query_via_client(        %% select previous/current client vsn, depending on
                          Query, Node, Config),  %% whether Node is downgraded or not
                  case ts_util:assert_float("query " ++ integer_to_list(TestNo), Exp, Got) of
                      pass ->
                          true;
                      fail ->
                          ct:log("failed query ~b issued at node ~b (~p)",
                                 [TestNo, NodeNo, rtdev:node_version(rtdev:node_id(Node))]),
                          false
                  end
          end,
          Nodes),
    if Success ->
            pass;
       el/=se ->
            ct:fail("some queries failed (see above)")
    end.


client_node(current, _Config) ->
    node();
client_node(previous, Config) ->
    ?CFG(previous_client_node, Config).

query_via_client(Query, Node, Config) ->
    VersionSlot = rtdev:node_version(
                    rtdev:node_id(Node)),
    ct:log("using ~s client with ~p to issue\n  ~s", [VersionSlot, Node, Query]),
    Client = rt:pbc(Node),
    case VersionSlot of
        current ->
            riakc_ts:query(Client, Query);
        previous ->
            rpc:call(
              client_node(VersionSlot, Config),
              riakc_ts, query, [Client, Query])
    end.

%%
%% helper fns
%%
count_non_nulls(Col) ->
    length([V || V <- Col, V =/= []]).

stddev_fun_builder(Avg) ->
    fun(X, Acc) -> Acc + (Avg-X)*(Avg-X) end.
