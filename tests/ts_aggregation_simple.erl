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
    {_, Got} = ts_util:ts_query(ClusterConn, TestType, DDL, Data, Qry, Bucket),
    ?assertEqual(Count, Got),

    Qry2 = "SELECT COUNT(timestamp) FROM " ++ Bucket,
    {_, Got2} = ts_util:single_query(Conn, Qry2),
    ?assertEqual(Count, Got2),

    Qry3 = "SELECT COUNT(pressure), COUNT(temperature), COUNT(precipitation) FROM " ++ Bucket,
    {_, Got3} = ts_util:single_query(Conn, Qry3),
    ?assertEqual([Count, Count, Count], Got3),

    Qry4 = "SELECT SUM(temperature) FROM " ++ Bucket,
    {_, Got4} = ts_util:single_query(Conn, Qry4),
    Sum4 = lists:sum(Column4),
    ?assertEqual(Sum4, Got4),

    Qry5 = "SELECT SUM(temperature), SUM(pressure), SUM(\precipitation) FROM " ++ Bucket,
    {_, Got5} = ts_util:single_query(Conn, Qry5),
    Sum5 = lists:sum(Column5),
    Sum6 = lists:sum(Column6),
    ?assertEqual([Sum4, Sum5, Sum6], Got5),

    Qry6 = "SELECT MIN(temperature), MIN(pressure) FROM " ++ Bucket,
    {_, Got6} = ts_util:single_query(Conn, Qry6),
    Min4 = lists:min(Column4),
    Min5 = lists:min(Column5),
    ?assertEqual([Min4, Min5], Got6),

    Qry7 = "SELECT MAX(temperature), MAX(pressure) FROM " ++ Bucket,
    {_, Got7} = ts_util:single_query(Conn, Qry7),
    Max4 = lists:max(Column4),
    Max5 = lists:max(Column5),
    ?assertEqual([Max4, Max5], Got7),

    Avg4 = Sum4 / Count,
    Avg5 = Sum5 / Count,
    Qry8 = "SELECT AVG(temperature), MEAN(pressure) FROM " ++ Bucket,
    {_, Got8} = ts_util:single_query(Conn, Qry8),
    ?assertEqual([Avg4, Avg5], Got8),

    StdDevFun4 = stddev_fun_builder(Avg4),
    StdDevFun5 = stddev_fun_builder(Avg5),
    StdDev4 = math:sqrt(lists:foldl(StdDevFun4, 0, Column4) / (Count-1)),
    StdDev5 = math:sqrt(lists:foldl(StdDevFun5, 0, Column5) / (Count-1)),
    Qry9 = "SELECT STDDEV(temperature), STDDEV(pressure) FROM " ++ Bucket,
    {_, Got9} = ts_util:single_query(Conn, Qry9),
    ?assertEqual([StdDev4, StdDev5], Got9),

    Qry10 = "SELECT SUM(temperature), MIN(pressure), AVG(pressure) FROM " ++ Bucket,
    {_, Got10} = ts_util:single_query(Conn, Qry10),
    ?assertEqual([Sum4, Min5, Avg5], Got10).










