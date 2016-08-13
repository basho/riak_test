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

-module(ts_simple_insert_incorrect_columns).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    DDL = ts_util:get_ddl(),
    Table = ts_util:get_default_bucket(),
    Data = ts_util:get_valid_select_data(),
    TooMuchData = [list_to_tuple([<<"rubbish">> | tuple_to_list(Row)]) || Row <- Data],
    TooLittleData = [list_to_tuple(lists:reverse(tl(lists:reverse(tuple_to_list(Row))))) || Row <- Data],
    WrongColumns = TooMuchData ++ TooLittleData,
    Columns = ts_util:get_cols(),

    {_Cluster, Conn} = ts_util:cluster_and_connect(single),
    ?assertEqual({ok, {[], []}}, riakc_ts:query(Conn, DDL)),

    Fn = fun(Datum, Acc) ->
                 [ts_util:ts_insert(Conn, Table, Columns, Datum) | Acc]
         end,
    Got2 = lists:reverse(lists:foldl(Fn, [], WrongColumns)),
    ?assertEqual(
        lists:duplicate(length(TooMuchData),
                        {error,{1018,<<"too many values in row index(es) 1">>}}) ++
        lists:duplicate(length(TooLittleData),
                        {error,{1003,<<"Invalid data found at row index(es) 1">>}}),
        Got2
    ),
    pass.
