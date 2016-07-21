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
-module(ts_cluster_updowngrade_select_aggregation_SUITE).

-export([
         suite/0,
         init_per_suite/1,
         end_per_suite/1,
         all/0,
         run_this_test/1
        ]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-compile(export_all).
-endif.

-include_lib("common_test/include/ct.hrl").
-include("ts_updown_util.hrl").

-define(TABLE, "updown_aggregation_test_table").
-define(TEMPERATURE_COL_INDEX, 4).
-define(PRESSURE_COL_INDEX, 5).
-define(PRECIPITATION_COL_INDEX, 6).

%% Callbacks

suite() ->
    [{timetrap, {seconds, 9000}}].

all() ->
    [
     run_this_test
    ].

init_per_suite(Config) ->
    lists:foldl(
      fun(Fun, Cfg) -> Fun(Cfg) end,
      Config,
      [fun ts_updown_util:setup/1,
       fun make_scenario_invariants/1]).

end_per_suite(Config) ->
    ts_updown_util:maybe_shutdown_client_node(Config).


-ifdef(EQC).

%% it's 2 to the power of the number of binary scenario parameters in
%% #scenario{} (6)
-define(NUMCASES, 64).

run_this_test(Config) ->
    case eqc:quickcheck(
           eqc:numtests(?NUMCASES, prop_scenario(Config))) of
        true ->
            pass;
        _ ->
            ct:fail("There were failing queries", [])
    end.

prop_scenario(Config) ->
    ?FORALL(
       Sce, gen_scenario(),
       [] == ts_updown_util:run_scenario(Config, Sce)).

gen_scenario() ->
    ?LET({TableNodeVsn, QueryNodeVsn,
          NeedTableNodeTransition, NeedQueryNodeTransition,
          NeedPreClusterMixed, NeedPostClusterMixed,
          Table},
         {gen_version(), gen_version(),
          bool(), bool(), bool(), bool(),
          gen_table()},
         #scenario{table_node_vsn = TableNodeVsn,
                   query_node_vsn = QueryNodeVsn,
                   need_table_node_transition = NeedTableNodeTransition,
                   need_query_node_transition = NeedQueryNodeTransition,
                   need_pre_cluster_mixed = NeedPreClusterMixed,
                   need_post_cluster_mixed = NeedPostClusterMixed,
                   table = Table,
                   ddl = ts_util:get_ddl(aggregation, binary_to_list(Table))}).

gen_version() ->
    ?LET(A, oneof([current, previous]), A).

gen_table() ->
    ?LET(A, ts_sql_eqc_util:gen_name("aggregation_test_"), A).


-else.  %% no EQC: produce a simple, single scenario

run_this_test(Config) ->
    Got = ts_updown_util:run_scenarios(Config, make_scenarios()),
    case Got of
        [] ->
            pass;
        Failures ->
            PrintMe = ts_updown_util:layout_fails_for_printing(Failures),
            ct:print("Failing queries:\n"
                     "----------------\n"
                     "~s\n", [PrintMe]),
            fail
    end.

make_scenarios() ->
    [#scenario{table_node_vsn = current,
               query_node_vsn = current,
               need_table_node_transition = true,
               need_query_node_transition = false,
               need_pre_cluster_mixed = false,
               need_post_cluster_mixed = false,
               table = <<?TABLE>>
               %% store these scenario invariants in Config:
               %% data = ?CFG(data, Config),
               %% create_query = DDL,
               %% select_vs_expected = ?CFG(data, Config)
              }].

-endif.


make_scenario_invariants(Config) ->
    DDL = ts_util:get_ddl(aggregation, ?TABLE),
    {SelectVsExpected, Data} = make_queries_and_data(),
    Config ++
        [
         {table, <<?TABLE>>},
         {data, Data},
         {ddl, DDL},
         {select_vs_expected, SelectVsExpected}
        ].

make_queries_and_data() ->
    Count = 10,
    Data = ts_util:get_valid_aggregation_data(Count),
    Column4 = [element(?TEMPERATURE_COL_INDEX,   X) || X <- Data],
    Column5 = [element(?PRESSURE_COL_INDEX,      X) || X <- Data],
    Column6 = [element(?PRECIPITATION_COL_INDEX, X) || X <- Data],

    %% now let's create some queries with their expected results
    Where =
        "WHERE myfamily = 'family1' and myseries = 'seriesX'"
        " and time >= 1 and time <= " ++ integer_to_list(Count),

    %% some preliminaries
    Sum4 = lists:sum([X || X <- Column4, is_number(X)]),
    Sum5 = lists:sum([X || X <- Column5, is_number(X)]),
    Sum6 = lists:sum([X || X <- Column6, is_number(X)]),
    Min4 = lists:min([X || X <- Column4, is_number(X)]),
    Min5 = lists:min([X || X <- Column5, is_number(X)]),
    Max4 = lists:max([X || X <- Column4, is_number(X)]),
    Max5 = lists:max([X || X <- Column5, is_number(X)]),

    C4 = [X || X <- Column4, is_number(X)],
    C5 = [X || X <- Column5, is_number(X)],
    Count4 = length(C4),
    Count5 = length(C5),
    Avg4 = Sum4 / Count4,
    Avg5 = Sum5 / Count5,

    StdDevFun4 = stddev_fun_builder(Avg4),
    StdDevFun5 = stddev_fun_builder(Avg5),
    StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / Count4),
    StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / Count5),
    Sample4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / (Count4-1)),
    Sample5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / (Count5-1)),

    QQEE =
        [{"SELECT COUNT(myseries) FROM ~s " ++ Where,
          {[<<"COUNT(myseries)">>], [{Count}]}},

         {"SELECT COUNT(time) FROM ~s " ++ Where,
          {[<<"COUNT(time)">>], [{Count}]}},

         {"SELECT COUNT(pressure), count(temperature), cOuNt(precipitation) FROM ~s " ++ Where,
          {[<<"COUNT(pressure)">>,
            <<"COUNT(temperature)">>,
            <<"COUNT(precipitation)">>],
           [{count_non_nulls(Column5),
             count_non_nulls(Column4),
             count_non_nulls(Column6)}]}},

         {"SELECT SUM(temperature) FROM ~s " ++ Where,
          {[<<"SUM(temperature)">>], [{lists:sum([X || X <- Column4, is_number(X)])}]}},

         {"SELECT SUM(temperature), sum(pressure), sUM(precipitation) FROM ~s " ++ Where,
          {[<<"SUM(temperature)">>, <<"SUM(pressure)">>, <<"SUM(precipitation)">>],
           [{Sum4, Sum5, Sum6}]}},

         {"SELECT MIN(temperature), MIN(pressure) FROM ~s " ++ Where,
          {[<<"MIN(temperature)">>, <<"MIN(pressure)">>], [{Min4, Min5}]}},

         {"SELECT MAX(temperature), MAX(pressure) FROM ~s " ++ Where,
          {[<<"MAX(temperature)">>, <<"MAX(pressure)">>], [{Max4, Max5}]}},

         {"SELECT AVG(temperature), MEAN(pressure) FROM ~s " ++ Where,
          {[<<"AVG(temperature)">>, <<"MEAN(pressure)">>],
           [{Avg4, Avg5}]}},

         {"SELECT STDDEV_POP(temperature), STDDEV_POP(pressure),"
          " STDDEV(temperature), STDDEV(pressure), "
          " STDDEV_SAMP(temperature), STDDEV_SAMP(pressure) FROM ~s " ++ Where,
          {[
            <<"STDDEV_POP(temperature)">>, <<"STDDEV_POP(pressure)">>,
            <<"STDDEV(temperature)">>, <<"STDDEV(pressure)">>,
            <<"STDDEV_SAMP(temperature)">>, <<"STDDEV_SAMP(pressure)">>
           ],
           [{StdDev4, StdDev5, Sample4, Sample5, Sample4, Sample5}]
          }},

         {"SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM ~s " ++ Where,
          {[<<"SUM(temperature)">>, <<"MIN(pressure)">>, <<"AVG(pressure)">>],
           [{Sum4, Min5, Avg5}]
          }}
        ],

    {[{N, {Q, {ok, Val}}} || {N, {Q, Val}} <- lists:zip(lists:seq(1, Count), QQEE)],
     Data}.


count_non_nulls(Col) ->
    length([V || V <- Col, V =/= []]).

stddev_fun_builder(Avg) ->
    fun(X, Acc) -> Acc + (Avg-X)*(Avg-X) end.
