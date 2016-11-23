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

-module(ts_simple_aggregation).

-behavior(riak_test).

-export([
         confirm/0,
         verify_aggregation/1
        ]).

-include_lib("eunit/include/eunit.hrl").

-define(TEMPERATURE_COL_INDEX, 4).
-define(PRESSURE_COL_INDEX, 5).
-define(PRECIPITATION_COL_INDEX, 6).

%% Test basic aggregation functions

confirm() ->
    verify_aggregation(single),
    pass.

stddev_fun_builder(Avg) ->
    fun(X, Acc) -> Acc + (Avg-X)*(Avg-X) end.

test_name(ClusterType, Name) ->
  lists:flatten(io_lib:format("~p:~p", [atom_to_list(ClusterType), Name])).

verify_aggregation(ClusterType) ->

    Size = case ClusterType of
        single ->
            1;
        multiple ->
            3;
        one_down ->
            3;
        delayed_one_down ->
            3;
        _ ->
            1
    end,

    DDL = ts_data:get_ddl(aggregation),
    lager:info("DDL is ~p", [DDL]),

    Cluster = ts_setup:start_cluster(Size),
    Conn = ts_setup:conn(Cluster),
    Count = 10,
    Data = ts_data:get_valid_aggregation_data(Count),
    lager:info("Data is ~p", [Data]),
    Column4 = [element(?TEMPERATURE_COL_INDEX, X) || X <- Data],
    Column5 = [element(?PRESSURE_COL_INDEX, X) || X <- Data],
    Column6 = [element(?PRECIPITATION_COL_INDEX, X) || X <- Data],
    Bucket = "WeatherData",

    Where = " WHERE myfamily = 'family1' and myseries = 'seriesX' and time >= 1 and time <= 10",

    Qry = "SELECT COUNT(myseries) FROM " ++ Bucket ++ Where,

    ts_setup:create_bucket_type(Cluster, DDL, Bucket),
    ts_setup:activate_bucket_type(Cluster, Bucket),

    %% Degraded clusters need to have DDL applied BEFORE taking down a node
    case ClusterType of
        delayed_one_down ->
            rt:stop(hd(tl(Cluster)));
        one_down ->
            rt:stop(hd(tl(Cluster)));
        _ -> ok
    end,
    ok = riakc_ts:put(Conn, Bucket, Data),
    Got = ts_ops:query(Cluster, Qry),
    Expected = {ok, {[<<"COUNT(myseries)">>], [{Count}]}},
    Result = ts_data:assert(test_name(ClusterType, "Count Strings"), Expected, Got),

    Qry2 = "SELECT COUNT(time) FROM " ++ Bucket ++ Where,
    Got2 = ts_ops:query(Cluster, Qry2),
    Expected2 = {ok, {[<<"COUNT(time)">>], [{Count}]}},
    Result2 = ts_data:assert(test_name(ClusterType, "Count Timestamps"), Expected2, Got2),

    Qry3 = "SELECT COUNT(pressure), count(temperature), cOuNt(precipitation) FROM " ++ Bucket ++ Where,
    Got3 = ts_ops:query(Cluster, Qry3),
    Expected3 = {ok, {
      [<<"COUNT(pressure)">>,
       <<"COUNT(temperature)">>,
       <<"COUNT(precipitation)">>
      ],
      [{count_non_nulls(Column5),
        count_non_nulls(Column4),
        count_non_nulls(Column6)}]}},
    Result3 = ts_data:assert(test_name(ClusterType, "Count Multiple Floats"), Expected3, Got3),

    Qry4 = "SELECT SUM(temperature) FROM " ++ Bucket ++ Where,
    Got4 = ts_ops:query(Cluster, Qry4),
    Sum4 = lists:sum([X || X <- Column4, is_number(X)]),
    Expected4 = {ok, {[<<"SUM(temperature)">>],
                 [{Sum4}]}},
    Result4 = ts_data:assert(test_name(ClusterType, "Single Float Sum"), Expected4, Got4),

    Qry5 = "SELECT SUM(temperature), sum(pressure), sUM(precipitation) FROM " ++ Bucket ++ Where,
    Got5 = ts_ops:query(Cluster, Qry5),
    Sum5 = lists:sum([X || X <- Column5, is_number(X)]),
    Sum6 = lists:sum([X || X <- Column6, is_number(X)]),
    Expected5 = {ok, {[<<"SUM(temperature)">>, <<"SUM(pressure)">>, <<"SUM(precipitation)">>],
                 [{Sum4, Sum5, Sum6}]}},
    Result5 = ts_data:assert(test_name(ClusterType, "Multiple Float Sums"), Expected5, Got5),

    Qry6 = "SELECT MIN(temperature), MIN(pressure) FROM " ++ Bucket ++ Where,
    Got6 = ts_ops:query(Cluster, Qry6),
    Min4 = lists:min([X || X <- Column4, is_number(X)]),
    Min5 = lists:min([X || X <- Column5, is_number(X)]),
    Expected6 = {ok, {[<<"MIN(temperature)">>, <<"MIN(pressure)">>],
                 [{Min4, Min5}]}},
    Result6 = ts_data:assert(test_name(ClusterType, "Min Floats"), Expected6, Got6),

    Qry7 = "SELECT MAX(temperature), MAX(pressure) FROM " ++ Bucket ++ Where,
    Got7 = ts_ops:query(Cluster, Qry7),
    Max4 = lists:max([X || X <- Column4, is_number(X)]),
    Max5 = lists:max([X || X <- Column5, is_number(X)]),
    Expected7 = {ok, {[<<"MAX(temperature)">>, <<"MAX(pressure)">>],
                 [{Max4, Max5}]}},
    Result7 = ts_data:assert(test_name(ClusterType, "Max Floats"), Expected7, Got7),

    C4 = [X || X <- Column4, is_number(X)],
    C5 = [X || X <- Column5, is_number(X)],
    Count4 = length(C4),
    Count5 = length(C5),

    Avg4 = Sum4 / Count4,
    Avg5 = Sum5 / Count5,
    Qry8 = "SELECT AVG(temperature), MEAN(pressure) FROM " ++ Bucket ++ Where,
    Got8 = ts_ops:query(Cluster, Qry8),
    Expected8 = {ok, {[<<"AVG(temperature)">>, <<"MEAN(pressure)">>],
                 [{Avg4, Avg5}]}},
    Result8 = ts_data:assert(test_name(ClusterType, "Avg and Mean"), Expected8, Got8),

    StdDevFun4 = stddev_fun_builder(Avg4),
    StdDevFun5 = stddev_fun_builder(Avg5),
    StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / Count4),
    StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / Count5),
    Sample4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / (Count4-1)),
    Sample5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / (Count5-1)),
    POPSTD = 2.8722813232690143, %%this is the std of 1-10, calculated using numpy
    Qry9 = "SELECT STDDEV_POP(temperature), STDDEV_POP(pressure)," ++
           " STDDEV(temperature), STDDEV(pressure), " ++
           " STDDEV_SAMP(temperature), STDDEV_SAMP(pressure)," ++
           " STDDEV_POP(time) FROM " ++ Bucket ++ Where,
    Got9 = ts_ops:query(Cluster, Qry9),
    Expected9 = {ok, {[<<"STDDEV_POP(temperature)">>, <<"STDDEV_POP(pressure)">>,
                  <<"STDDEV(temperature)">>, <<"STDDEV(pressure)">>,
                  <<"STDDEV_SAMP(temperature)">>, <<"STDDEV_SAMP(pressure)">>, <<"STDDEV_POP(time)">>],
                 [{StdDev4, StdDev5, Sample4, Sample5, Sample4, Sample5, POPSTD}]}},
    Result9 = ts_data:assert_float(test_name(ClusterType, "Standard Deviation"), Expected9, Got9),

    Qry10 = "SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM " ++ Bucket ++ Where,
    Got10 = ts_ops:query(Cluster, Qry10),
    Expected10 = {ok, {[<<"SUM(temperature)">>, <<"MIN(pressure)">>, <<"AVG(pressure)">>],
                  [{Sum4, Min5, Avg5}]}},
    Result10 = ts_data:assert(test_name(ClusterType, "Mixter Maxter"), Expected10, Got10),

    ts_data:results([
             Result,
             Result2,
             Result3,
             Result4,
             Result5,
             Result6,
             Result7,
             Result8,
             Result9,
             Result10
            ]),

    riakc_pb_socket:stop(Conn),
    Cluster.

count_non_nulls(Col) ->
    length([V || V <- Col, V =/= []]).

