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

-module(ts_aggregation_math).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

% Ensure aggregation functions only work on desired data types

confirm() ->
    DDL = ts_util:get_ddl(aggregration),
    Count = 10,
    Data = ts_util:get_valid_aggregation_data(Count),
    Column4 = [lists:nth(4, X) || X <- Data],
    Column5 = [lists:nth(5, X) || X <- Data],
    Column6 = [lists:nth(6, X) || X <- Data],
    TestType = normal,
    Bucket = "WeatherData",

    Qry = "SELECT AVG(temperature) * (9/5) + 32 FROM " ++ Bucket ++ " WHERE temperature > 10",
    ClusterConn = {_Cluster, Conn} = ts_util:cluster_and_connect(single),
    FilteredTemp = lists:filter(fun(X) -> case X>10 of true -> true; _ -> false end end, Column4),
    FilteredSum4 = lists:sum(FilteredTemp),
    {_, Got} = ts_util:ts_query(ClusterConn, TestType, DDL, Data, Qry, Bucket),
    ?assertEqual(Got, (FilteredSum4/length(FilteredTemp)) * (9/5) + 32),

    Qry2 = "SELECT SUM(pressure/precipitation) FROM " ++ Bucket,
    {_, Got2} = ts_util:single_query(Conn, Qry2),
    SumDiv = lists:sum([Press/Precip || {Press, Precip} <- lists:zip(Column5, Column6)]),
    ?assertEqual(Got2, SumDiv),

    Qry3 = "SELECT 3+5, 2.0+8, 9/2, 9.0/2 FROM " ++ Bucket,
    {_, Got3} = ts_util:single_query(Conn, Qry3),
    Math = [[8, 10.0, 4, 4.5] || _ <- lists:seq(1, Count)],
    ?assertEqual(Got3, Math),

    Qry4 = "SELECT SUM(temperature+10), AVG(pressure)/10 FROM " ++ Bucket,
    {_, Got4} = ts_util:single_query(Conn, Qry4),
    SumPlus = lists:sum([X+10 || X<-Column4]),
    AvgDiv = lists:sum(Column5)/Count/10,
    ?assertEqual(Got4, [SumPlus, AvgDiv]),
    pass.




