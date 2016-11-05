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

-module(ts_simple_aggregation_math).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

% Ensure aggregation functions only work on desired data types

confirm() ->
    DDL = ts_data:get_ddl(aggregation),
    Count = 10,
    Data = ts_data:get_valid_aggregation_data_not_null(Count),
    Column4 = [element(4, X) || X <- Data],
    Column5 = [element(5, X) || X <- Data],
    Column6 = [element(6, X) || X <- Data],
    Bucket = "WeatherData",

    Where = " WHERE myfamily = 'family1' and myseries = 'seriesX' and time >= 1 and time <= 10",

    Qry = "SELECT AVG(temperature) * (9/5) + 32 FROM " ++ Bucket ++ Where ++ "  and temperature > 10",

    Cluster = ts_setup:start_cluster(1),

    FilteredTemp = lists:filter(fun(X) -> case X>10 andalso is_number(X) of true -> true; _ -> false end end, Column4),
    _FilteredSum4 = lists:sum(FilteredTemp),

    {ok,_} = ts_setup:create_bucket_type(Cluster, DDL, Bucket),
    ok = ts_setup:activate_bucket_type(Cluster, Bucket),
    ok = ts_ops:put(Cluster, Bucket, Data),
    {_, {_, _Got}} = ts_ops:query(Cluster, Qry),

    %% ?assertEqual((FilteredSum4/length(FilteredTemp)) * (9/5) + 32, Got),

    Qry2 = "SELECT SUM(pressure/precipitation) FROM " ++ Bucket ++ Where,
    {ok, {_, Got2}} = ts_ops:query(Cluster, Qry2),
    SumDiv = lists:sum(
        [Press/Precip || {Press, Precip} <- lists:zip(Column5, Column6), Press /= [], Precip /= []]),
    ?assertEqual([{SumDiv}], Got2),

    Qry3 = "SELECT 3+5, 2.0+8, 9/2, 9.0/2 FROM " ++ Bucket ++ Where,
    {ok, {_, Got3}} = ts_ops:query(Cluster, Qry3),
    Arithmetic = [{8, 10.0, 4, 4.5} || _ <- lists:seq(1, Count)],
    ?assertEqual(Arithmetic, Got3),

    Qry4 = "SELECT SUM(temperature+10), AVG(pressure)/10 FROM " ++ Bucket ++ Where,
    {ok, {_, Got4}} = ts_ops:query(Cluster, Qry4),
    SumPlus = lists:sum([X+10 || X<-Column4]),
    AvgDiv = lists:sum(Column5)/Count/10,
    ?assertEqual([{SumPlus, AvgDiv}], Got4),

    div_by_zero_test(Cluster, Bucket, Where),

    div_aggregate_function_by_zero_test(Cluster, Bucket, Where),

    negate_an_aggregation_test(Cluster, Bucket, Where),

    pass.

%%
div_by_zero_test(Cluster, Bucket, Where) ->
    Query = "SELECT 5 / 0 FROM " ++ Bucket ++ Where,
    ?assertEqual(
        {error,{1001,<<"Divide by zero">>}},
        ts_ops:query(Cluster, Query)
    ).

%%
div_aggregate_function_by_zero_test(Cluster, Bucket, Where) ->
    Query = "SELECT COUNT(*) / 0 FROM " ++ Bucket ++ Where,
    ?assertEqual(
        {error,{1001,<<"Divide by zero">>}},
        ts_ops:query(Cluster, Query)
    ).

%%
negate_an_aggregation_test(Cluster, Bucket, Where) ->
    Query = "SELECT -COUNT(*), COUNT(*) FROM " ++ Bucket ++ Where,
    ?assertEqual(
        {ok, {[<<"-COUNT(*)">>, <<"COUNT(*)">>],[{-10, 10}]}},
        ts_ops:query(Cluster, Query)
    ).
