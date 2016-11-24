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
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),
    Data = ts_data:get_valid_select_data(),
    TooMuchData = [list_to_tuple(tuple_to_list(Row) ++ [<<"rubbish">>]) || Row <- Data],
    %% remove the last 2 columns to chomp into a not null field because it is
    %% completely valid to drop the final 1 column that is nullable.
    TooLittleData = [list_to_tuple(lists:reverse(tl(tl(lists:reverse(tuple_to_list(Row)))))) || Row <- Data],
    WrongColumns = TooMuchData ++ TooLittleData,
    Columns = ts_data:get_cols(),

    Cluster = ts_setup:start_cluster(1),
    ?assertEqual({ok, {[], []}}, ts_ops:query(Cluster, DDL)),

    Fn = fun(Datum, Acc) ->
                 [ts_ops:insert(Cluster, Table, Columns, Datum) | Acc]
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
