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

-module(ts_simple_insert).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    DDL = ts_util:get_ddl(),
    Table = ts_util:get_default_bucket(),
    Columns = ts_util:get_cols(),
    {Cluster, Conn} = ts_util:cluster_and_connect(single),
    Got = ts_util:create_and_activate_bucket_type(Cluster, DDL),
    ExpectedActivationMessage = Table ++ " has been activated\n",
    ?assertEqual(ok, element(1, Got)),
    ?assertEqual(ExpectedActivationMessage,
        lists:sublist(element(2, Got), 1, length(ExpectedActivationMessage))),

    Data1 = ts_util:get_valid_select_data(),
    Insert1Fn = fun(Datum, Acc) ->
                   [ts_util:ts_insert(Conn, Table, Columns, Datum) | Acc]
               end,
    Got1 = lists:reverse(lists:foldl(Insert1Fn, [], Data1)),
    Expected1 = lists:duplicate(10, {ok,{[],[]}}),
    Result1 = ts_util:assert("Insert With Columns", Expected1, Got1),

    Qry2 = "select * from GeoCheckin Where time >= 1 and time <= 10 and myfamily = 'family1' and myseries ='seriesX'",
    Got2 = ts_util:single_query(Conn, Qry2),
    Expected2 = {ok, {Columns, ts_util:exclusive_result_from_data(Data1, 1, 10)}},
    Result2 = ts_util:assert("Insert With Columns (results)", Expected2, Got2),

    Data3 = ts_util:get_valid_select_data(fun() -> lists:seq(11, 20) end),
    Insert3Fn = fun(Datum, Acc) ->
                    [ts_util:ts_insert_no_columns(Conn, Table, Datum) | Acc]
               end,
    Got3 = lists:reverse(lists:foldl(Insert3Fn, [], Data3)),
    Result3 = ts_util:assert("Insert Without Columns", Expected1, Got3),

    Qry4 = "select * from GeoCheckin Where time >= 11 and time <= 20 and myfamily = 'family1' and myseries ='seriesX'",
    Got4 = ts_util:single_query(Conn, Qry4),
    Expected4 = {ok, {Columns, ts_util:exclusive_result_from_data(Data3, 1, 10)}},
    Result4 = ts_util:assert("Insert Without Columns (results)", Expected4, Got4),

    %% inserting columns out of order and partial, excluding temperature
    Columns5 = [<<"myfamily">>, <<"time">>, <<"weather">>, <<"myseries">>],
    Data5 = [ {
            <<"family1">>, %%<< myfamily
            I, %%<< time
            <<"nully">>, %%<< weather
            <<"seriesX">> %%<< myseries
            } || I <- lists:seq(21, 30) ],

    Insert5Fn = fun(Datum, Acc) ->
                    [ts_util:ts_insert(Conn, Table, Columns5, Datum) | Acc]
            end,
    Got5 = lists:reverse(lists:foldl(Insert5Fn, [], Data5)),
    Expected5 = [ {ok,{[],[]}} || _I <- lists:seq(21, 30) ],
    Result5 = ts_util:assert("Insert with NULL (results)", Expected5, Got5),

    ts_util:results([
        Result1,
        Result2,
        Result3,
        Result4,
        Result5
    ]),
    pass.
