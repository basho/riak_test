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

%% Test basic aggregation functions

confirm() ->
    verify_aggregation(single),
    pass.

stddev_fun_builder(Avg) ->
    fun(X, Acc) -> Acc + (Avg-X)*(Avg-X) end.

verify_aggregation(ClusterType) ->
    DDL = ts_util:get_ddl(aggregration),
    Count = 10,
    Data = ts_util:get_valid_aggregation_data(Count),
    Column4 = [lists:nth(4, X) || X <- Data],
    Column5 = [lists:nth(5, X) || X <- Data],
    Column6 = [lists:nth(6, X) || X <- Data],
    TestType = normal,
    Bucket = "WeatherData",

    Qry = "SELECT COUNT(myseries) FROM " ++ Bucket,
    ClusterConn = {_Cluster, Conn} = ts_util:cluster_and_connect(ClusterType),
    Got = ts_util:ts_query(ClusterConn, TestType, DDL, Data, Qry, Bucket),
    ?assertEqual(Got, Count),

    Qry2 = "SELECT COUNT(timestamp) FROM " ++ Bucket,
    Got2 = ts_util:single_query(Conn, Qry2),
    ?assertEqual(Got2, Count),

    Qry3 = "SELECT COUNT(pressure), COUNT(temperature), COUNT(precipitation) FROM " ++ Bucket,
    Got3 = ts_util:single_query(Conn, Qry3),
    ?assertEqual(Got3, [Count, Count, Count]),

    Qry4 = "SELECT SUM(temperature) FROM " ++ Bucket,
    Got4 = ts_util:single_query(Conn, Qry4),
    Sum4 = lists:sum(Column4),
    ?assertEqual(Got4, Sum4),

    Qry5 = "SELECT SUM(temperature), SUM(pressure), SUM(\precipitation) FROM " ++ Bucket,
    Got5 = ts_util:single_query(Conn, Qry5),
    Sum5 = lists:sum(Column5),
    Sum6 = lists:sum(Column6),
    ?assertEqual(Got5, [Sum4, Sum5, Sum6]),

    Qry6 = "SELECT MIN(temperature), MIN(pressure) FROM " ++ Bucket,
    Got6 = ts_util:single_query(Conn, Qry6),
    Min4 = lists:min(Column4),
    Min5 = lists:min(Column5),
    ?assertEqual(Got6, [Min4, Min5]),

    Qry7 = "SELECT MAX(temperature), MAX(pressure) FROM " ++ Bucket,
    Got7 = ts_util:single_query(Conn, Qry7),
    Max4 = lists:max(Column4),
    Max5 = lists:max(Column5),
    ?assertEqual(Got7, [Max4, Max5]),

    Avg4 = Sum4 / Count,
    Avg5 = Sum5 / Count,
    Qry8 = "SELECT AVG(temperature), MEAN(pressure) FROM " ++ Bucket,
    Got8 = ts_util:single_query(Conn, Qry8),
    ?assertEqual(Got8, [Avg4, Avg5]),

    StdDevFun4 = stddev_fun_builder(Avg4),
    StdDevFun5 = stddev_fun_builder(Avg5),
    StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, Column4) / (Count-1)),
    StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, Column5) / (Count-1)),
    Qry9 = "SELECT STDDEV(temperature), STDDEV(pressure) FROM " ++ Bucket,
    Got9 = ts_util:single_query(Conn, Qry9),
    ?assertEqual(Got9, [StdDev4, StdDev5]),

    Qry10= "SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM " ++ Bucket,
    Got10 = ts_util:single_query(Conn, Qry10),
    ?assertEqual(Got10, [Sum4, Min5, Avg5]).










