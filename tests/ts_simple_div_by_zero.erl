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

-module(ts_simple_div_by_zero).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

%% Test handling of division by zero in a query select clause.
confirm() ->
    DDL = ts_util:get_ddl(aggregation),
    Data = ts_util:get_valid_aggregation_data_not_null(10),
    TestType = normal,
    {Cluster, ClientConn} = ts_util:cluster_and_connect(single),
    ts_util:create_table(TestType, Cluster, DDL, table()),
    ok = riakc_ts:put(ClientConn, table(), Data),

    TsQueryFn =
        fun(Query_x) ->
            ts_util:single_query(ClientConn, Query_x)
        end,
    arithmetic_int_div_int_zero_test(TsQueryFn),
    arithmetic_float_div_int_zero_test(TsQueryFn),
    arithmetic_int_div_float_zero_test(TsQueryFn),
    arithmetic_float_div_float_zero_test(TsQueryFn),
    arithmetic_div_by_zero_as_function_argument_test(TsQueryFn),
    pass.

%%
arithmetic_int_div_int_zero_test(TsQueryFn) ->
    Actual = TsQueryFn(
        "SELECT 2 / 0 FROM " ++ table() ++ where()),
    ?assertEqual(error_divide_by_zero(), Actual).

%%
arithmetic_float_div_int_zero_test(TsQueryFn) ->
    Actual = TsQueryFn(
        "SELECT 2.0 / 0 FROM " ++ table() ++ where()),
    ?assertEqual(error_divide_by_zero(), Actual).

%%
arithmetic_int_div_float_zero_test(TsQueryFn) ->
    Actual = TsQueryFn(
        "SELECT 2 / 0.0 FROM " ++ table() ++ where()),
    ?assertEqual(error_divide_by_zero(), Actual).

%%
arithmetic_float_div_float_zero_test(TsQueryFn) ->
    Actual = TsQueryFn(
        "SELECT 2.0 / 0.0 FROM " ++ table() ++ where()),
    ?assertEqual(error_divide_by_zero(), Actual).

%%
arithmetic_div_by_zero_as_function_argument_test(TsQueryFn) ->
    Actual = TsQueryFn(
        "SELECT SUM(2 / 0) FROM " ++ table() ++ where()),
    ?assertEqual(error_divide_by_zero(), Actual).

%%
table() ->
    "WeatherData".

%%
where() ->
    " "
    "WHERE myfamily = 'family1' AND myseries = 'seriesX' "
    "AND time >= 1 AND time <= 10 ".

error_divide_by_zero() ->
    {error,{1001,<<"divide_by_zero">>}}.
