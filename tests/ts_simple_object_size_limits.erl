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

-module(ts_simple_object_size_limits).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(MAX_OBJECT_SIZE, 500 * 1024).

confirm() ->
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),
    Columns = ts_data:get_cols(),

    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),

    Data1 = get_large_select_data(fun() -> lists:seq(11, 20) end),
    Insert1Fn = fun(Datum, Acc) ->
                   [ts_ops:insert(Cluster, Table, Columns, Datum) | Acc]
               end,
    Got1 = lists:reverse(lists:foldl(Insert1Fn, [], Data1)),
    Expected1 = lists:duplicate(10, {error,{1001,<<"Failed to put 1 record(s)">>}}),
    Result1 = ts_data:assert("Insert With Columns", Expected1, Got1),

    Qry2 = "select * from GeoCheckin Where time >= 1 and time <= 10 and myfamily = 'family1' and myseries ='seriesX'",
    Got2 = ts_ops:query(Cluster, Qry2),
    Expected2 = {ok, {[], []}},
    Result2 = ts_data:assert("Insert With Columns (results)", Expected2, Got2),

    Data3 = get_large_select_data(fun() -> lists:seq(11, 20) end),
    Insert3Fn = fun(Datum, Acc) ->
                    [ts_ops:insert_no_columns(Cluster, Table, Datum) | Acc]
               end,
    Got3 = lists:reverse(lists:foldl(Insert3Fn, [], Data3)),
    Result3 = ts_data:assert("Insert Without Columns", Expected1, Got3),

    Qry4 = "select * from GeoCheckin Where time >= 11 and time <= 20 and myfamily = 'family1' and myseries ='seriesX'",
    Got4 = ts_ops:query(Cluster, Qry4),
    Result4 = ts_data:assert("Insert Without Columns (results)", Expected2, Got4),

    Got5 = ts_ops:put(Cluster, Table, Data1),
    Expected5 = {error,{1004,<<"Failed to put 10 record(s)">>}},
    Result5 = ts_data:assert("Batch Put", Expected5, Got5),

    ts_data:results([
        Result1,
        Result2,
        Result3,
        Result4,
        Result5
    ]),
    pass.

get_large_select_data(SeqFun) ->
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = SeqFun(),
    [{Family, Series, X, get_large_varchar(), ts_data:get_float()} || X <- Times].

get_large_varchar() ->
    Len = random:uniform(10000) + ?MAX_OBJECT_SIZE,
    String = ts_data:get_string(Len),
    list_to_binary(String).
