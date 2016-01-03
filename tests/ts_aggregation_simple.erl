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

-module(ts_aggregation_simple).

-behavior(riak_test).

-export([
         confirm/0,
         verify_aggregation/1
        ]).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%% Test basic aggregation functions

confirm() ->
    verify_aggregation(single),
    pass.

stddev_fun_builder(Avg) ->
    fun(X, Acc) -> Acc + (Avg-X)*(Avg-X) end.

verify_aggregation(ClusterType) ->
    DDL = ts_util:get_ddl(aggregration),
    lager:info("DDL is ~p", [DDL]),

    ClusterConn = {_Cluster, Conn} = ts_util:cluster_and_connect(ClusterType),

    Count = 10,
    Data = ts_util:get_valid_aggregation_data(Count),
    lager:info("Data is ~p", [Data]),
    Column4 = [lists:nth(4, X) || X <- Data],
    Column5 = [lists:nth(5, X) || X <- Data],
    Column6 = [lists:nth(6, X) || X <- Data],
    TestType = normal,
    Bucket = "WeatherData",

    Where = " WHERE myfamily = 'family1' and myseries = 'seriesX' and time >= 1 and time <= 10",

    Qry = "SELECT COUNT(myseries) FROM " ++ Bucket ++ Where,
    Got = ts_util:ts_query(ClusterConn, TestType, DDL, Data, Qry, Bucket),
    Expected = {[<<"COUNT(myseries)">>], [{Count}]},
    Result = assert("Basic count", Expected, Got),

    Qry2 = "SELECT COUNT(time) FROM " ++ Bucket ++ Where,
    Got2 = ts_util:single_query(Conn, Qry2),
    Expected2 = {[<<"COUNT(time)">>], [{Count}]},
    Result2 = assert("Basic count 2", Expected2, Got2),

    Qry3 = "SELECT COUNT(pressure), count(temperature), cOuNt(precipitation) FROM " ++ Bucket ++ Where,
    Got3 = ts_util:single_query(Conn, Qry3),
    Expected3 = {
      [<<"COUNT(pressure)">>,
       <<"COUNT(temperature)">>,
       <<"COUNT(precipitation)">>
      ],
      [{Count, Count, Count}]},
     Result3 = assert("Many Counts", Expected3, Got3),

    Qry4 = "SELECT SUM(temperature) FROM " ++ Bucket ++ Where,
    Got4 = ts_util:single_query(Conn, Qry4),
    Sum4 = lists:sum([X || X <- Column4, is_number(X)]),
    Expected4 = {[<<"SUM(temperature)">>],
                 [{Sum4}]},
    Result4 = assert("Single Sum", Expected4, Got4),

    Qry5 = "SELECT SUM(temperature), sum(pressure), sUM(precipitation) FROM " ++ Bucket ++ Where,
    Got5 = ts_util:single_query(Conn, Qry5),
    Sum5 = lists:sum([X || X <- Column5, is_number(X)]),
    Sum6 = lists:sum([X || X <- Column6, is_number(X)]),
    Expected5 = {[<<"SUM(temperature)">>, <<"SUM(pressure)">>, <<"SUM(precipitation)">>],
                 [{Sum4, Sum5, Sum6}]},
    Result5 = assert("Many Sums", Expected5, Got5),

    Qry6 = "SELECT MIN(temperature), MIN(pressure) FROM " ++ Bucket ++ Where,
    Got6 = ts_util:single_query(Conn, Qry6),
    Min4 = lists:min([X || X <- Column4, is_number(X)]),
    Min5 = lists:min([X || X <- Column5, is_number(X)]),
    Expected6 = {[<<"MIN(temperature)">>, <<"MIN(pressure)">>],
                 [{Min4, Min5}]},
    Result6 = assert("Min", Expected6, Got6),

    Qry7 = "SELECT MAX(temperature), MAX(pressure) FROM " ++ Bucket ++ Where,
    Got7 = ts_util:single_query(Conn, Qry7),
    Max4 = lists:max([X || X <- Column4, is_number(X)]),
    Max5 = lists:max([X || X <- Column5, is_number(X)]),
    Expected7 = {[<<"MAX(temperature)">>, <<"MAX(pressure)">>],
                 [{Max4, Max5}]},
    Result7 = assert("Max", Expected7, Got7),

    Avg4 = Sum4 / Count,
    Avg5 = Sum5 / Count,
    io:format("Avg4 is ~p Avg5 is ~p~n", [Avg4, Avg5]),
    Qry8 = "SELECT AVG(temperature), MEAN(pressure) FROM " ++ Bucket ++ Where,
    Got8 = ts_util:single_query(Conn, Qry8),
    Expected8 = {[<<"AVG(temperature)">>, <<"MEAN(pressure)">>],
                 [{Avg4, Avg5}]},
    Result8 = assert("Avg and Mean", Expected8, Got8),


    %% StdDevFun4 = stddev_fun_builder(Avg4),
    %% StdDevFun5 = stddev_fun_builder(Avg5),
    %% C4 = [X || X <- Column4, is_number(X)],
    %% C5 = [X || X <- Column5, is_number(X)],
    %% Count4 = length(C4),
    %% Count5 = length(C5),
    %% StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, C4) / (Count4-1)),
    %% StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, C5) / (Count5-1)),
    %% Qry9 = "SELECT STDDEV(temperature), STDDEV(pressure) FROM " ++ Bucket ++ Where,
    %% Got9 = ts_util:single_query(Conn, Qry9),
    %% Expected9 = {[<<"STDDEV(temperature)">>, <<"STDEV(pressure)">>],
    %%              [{StdDev4, StdDev5}]},
    %% Result9 = assert("Standard Deviation", Expected9, Got9),

    %% Qry10 = "SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM " ++ Bucket ++ Where,
    %% Got10 = ts_util:single_query(Conn, Qry10),
    %% Expected10 = {[<<"SUM(temperature)">>, <<"MIN(pressure)">>, <<"AVG(pressure)">>],
    %%               [{Sum4, Min5, Avg5}]},
    %% Result10 = assert("Mixter Maxter", Expected10, Got10),

    results([
             Result,
             Result2,
             Result3,
             Result4,
             Result5,
             Result6,
             Result7,
             Result8
             %% Result9,
             %% Result10
            ]).

results(Results) ->
    Expected = lists:duplicate(length(Results), pass),
    ?assertEqual(Expected, Results).

assert(_,      X,   X)   -> pass;
assert(String, Exp, Got) -> lager:info("*****************", []),
                            lager:info("Test ~p failed", [String]),
                            lager:info("Exp ~p", [Exp]),
                            lager:info("Got ~p", [Got]),
                            lager:info("*****************", []),
                            fail.
