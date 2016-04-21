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
-module(ts_updown_util).

-include_lib("eunit/include/eunit.hrl").

-export([
         init_per_suite_data_write/1,
         do_node_transition/3,
         run_init_per_suite_queries/1,
         run_init_per_suite_queries/3
        ]).

-define(TEMPERATURE_COL_INDEX, 4).
-define(PRESSURE_COL_INDEX, 5).
-define(PRECIPITATION_COL_INDEX, 6).

init_per_suite_data_write(Nodes) ->
    StartingNode = hd(Nodes),
    Conn = rt:pbc(StartingNode),
    ?assert(is_pid(Conn)),

    AggTable = "Aggregation_written_on_old_cluster",
    DDL = ts_util:get_ddl(aggregation, AggTable),
    ct:pal("DDL is ~p~n", [DDL]),

    {ok, _} = ts_util:create_and_activate_bucket_type({Nodes, StartingNode}, DDL, AggTable),
    ct:pal("Table ~p created", [AggTable]),
    
    %% generate data and hoy it into the cluster
    Count = 10,
    Data = ts_util:get_valid_aggregation_data(Count),
    Column4 = [element(?TEMPERATURE_COL_INDEX,   X) || X <- Data],
    Column5 = [element(?PRESSURE_COL_INDEX,      X) || X <- Data],
    Column6 = [element(?PRECIPITATION_COL_INDEX, X) || X <- Data],
    ct:pal("Write ~p rows of data~n- ~p~n", [Count, Data]),
    ok = riakc_ts:put(Conn, AggTable, Data),

    %% now lets create some queries with their expected results
    Where = " WHERE myfamily = 'family1' and myseries = 'seriesX' " ++
        "and time >= 1 and time <= " ++ integer_to_list(Count),

    Qry1 = "SELECT COUNT(myseries) FROM " ++ AggTable ++ Where,
    Expected1 = {[<<"COUNT(myseries)">>], [{Count}]},

    Qry2 = "SELECT COUNT(time) FROM " ++ AggTable ++ Where,
    Expected2 = {[<<"COUNT(time)">>], [{Count}]},

    Qry3 = "SELECT COUNT(pressure), count(temperature), cOuNt(precipitation) FROM " ++ 
        AggTable ++ Where,
    Expected3 = {
      [<<"COUNT(pressure)">>,
       <<"COUNT(temperature)">>,
       <<"COUNT(precipitation)">>
      ],
      [{count_non_nulls(Column5),
        count_non_nulls(Column4),
        count_non_nulls(Column6)}]},

    Qry4 = "SELECT SUM(temperature) FROM " ++ AggTable ++ Where,
    Sum4 = lists:sum([X || X <- Column4, is_number(X)]),
    Expected4 = {[<<"SUM(temperature)">>],
                 [{Sum4}]},

    Qry5 = "SELECT SUM(temperature), sum(pressure), sUM(precipitation) FROM " ++ 
        AggTable ++ Where,
    Sum5 = lists:sum([X || X <- Column5, is_number(X)]),
    Sum6 = lists:sum([X || X <- Column6, is_number(X)]),
    Expected5 = {[<<"SUM(temperature)">>, <<"SUM(pressure)">>, <<"SUM(precipitation)">>],
                 [{Sum4, Sum5, Sum6}]},

    Qry6 = "SELECT MIN(temperature), MIN(pressure) FROM " ++ AggTable ++ Where,
    Min4 = lists:min([X || X <- Column4, is_number(X)]),
    Min5 = lists:min([X || X <- Column5, is_number(X)]),
    Expected6 = {[<<"MIN(temperature)">>, <<"MIN(pressure)">>],
                 [{Min4, Min5}]},

    Qry7 = "SELECT MAX(temperature), MAX(pressure) FROM " ++ AggTable ++ Where,
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
    Qry8 = "SELECT AVG(temperature), MEAN(pressure) FROM " ++ AggTable ++ Where,
    Expected8 = {[<<"AVG(temperature)">>, <<"MEAN(pressure)">>],
                 [{Avg4, Avg5}]},

    StdDevFun4 = stddev_fun_builder(Avg4),
    StdDevFun5 = stddev_fun_builder(Avg5),
    StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / Count4),
    StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / Count5),
    Sample4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / (Count4-1)),
    Sample5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / (Count5-1)),
    Qry9 = "SELECT STDDEV_POP(temperature), STDDEV_POP(pressure)," ++
           " STDDEV(temperature), STDDEV(pressure), " ++
           " STDDEV_SAMP(temperature), STDDEV_SAMP(pressure) FROM " ++ AggTable ++ Where,
    Expected9 = {
      [
       <<"STDDEV_POP(temperature)">>, <<"STDDEV_POP(pressure)">>,
       <<"STDDEV(temperature)">>, <<"STDDEV(pressure)">>,
       <<"STDDEV_SAMP(temperature)">>, <<"STDDEV_SAMP(pressure)">>
      ],
      [{StdDev4, StdDev5, Sample4, Sample5, Sample4, Sample5}]
     },

    Qry10 = "SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM " ++ 
        AggTable ++ Where,
    Expected10 = {
      [<<"SUM(temperature)">>, <<"MIN(pressure)">>, <<"AVG(pressure)">>],
      [{Sum4, Min5, Avg5}]
     },

    QueryConfig = [ 
                    {init_per_suite_queries,
                     [
                      {1,  {Qry1,  Expected1}},
                      {2,  {Qry2,  Expected2}},
                      {3,  {Qry3,  Expected3}},
                      {4,  {Qry4,  Expected4}},
                      {5,  {Qry5,  Expected5}},
                      {6,  {Qry6,  Expected6}},
                      {7,  {Qry7,  Expected7}},
                      {8,  {Qry8,  Expected8}},
                      {9,  {Qry9,  Expected9}},
                      {10, {Qry10, Expected10}}
                     ]
                    }
                  ],
    ct:pal("Queries and expected results created ~p", [QueryConfig]),
    QueryConfig.

do_node_transition(Config, N, Version) ->
    {nodes, Nodes} = lists:keyfind(nodes, 1, Config),
    {N, Node} = lists:keyfind(N, 1, Nodes),
    {Version, ToVsn} = lists:keyfind(Version, 1, Config),
    ok = rt:upgrade(Node, ToVsn),
    ok = rt:wait_for_service(Node, riak_kv),
    timer:sleep(5000),
    pass.

run_init_per_suite_queries(_) -> 
    "Run a set of queries against data written in the init_per_suite stage".

run_init_per_suite_queries(Config, ClientVsn, TestNo) when is_integer(TestNo) andalso 
                                                TestNo > 0 -> 
    {init_per_suite_queries, Queries} = lists:keyfind(init_per_suite_queries, 1, Config),
    {TestNo, {Query, Exp}} = lists:keyfind(TestNo, 1, Queries),
    ct:pal("gonnae run the query (~p ~p) ~p", [ClientVsn, TestNo, Query]),
    {nodes, Nodes} = lists:keyfind(nodes, 1, Config),
    {1, Node} = lists:keyfind(1, 1, Nodes),
    ct:pal("connecting to node (~p, ~p) ~p~n", [ClientVsn, TestNo, Node]),
    Conn = rt:pbc(Node),
    Opts = case ClientVsn of
               "1.2" -> [];
               "1.3" -> [{use_ttb, false}]
           end,
    Got = case {ClientVsn, ts_util:single_query(Conn, Query, Opts)} of
              {"1.2", G}       -> G;
              {"1.3", {ok, G}} -> G
          end,
    ct:pal("query returned (~p, ~p) ~p~n", [ClientVsn, TestNo, Got]),
    ok = dump_load_info(),
    ts_util:assert("reading data query " ++ integer_to_list(TestNo), Exp, Got).

%%
%% helper fns
%%
count_non_nulls(Col) ->
    length([V || V <- Col, V =/= []]).

stddev_fun_builder(Avg) ->
    fun(X, Acc) -> Acc + (Avg-X)*(Avg-X) end.

dump_load_info() ->
    Mods = [riakc_ts, protobuffer],
    [ok = dump2(Mod) || Mod <- Mods],
    ok.

dump2(Mod) ->
    case code:is_loaded(Mod) of
        {file, Loaded} ->
            ct:pal("The ~p loaded is ~p~n", [Mod, Loaded]);
        false ->
            ct:pal("The ~p is not loaded~n", [Mod])
    end.
