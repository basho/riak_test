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

-module(ts_simple_get).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

%% Test gets which return no data, i.e., not found.

confirm() ->
    DDL = ts_util:get_ddl(),
    Data = ts_util:get_valid_select_data(),
    DataRow = hd(Data),
    Key = lists:sublist(tuple_to_list(DataRow), 3),
    Expected = {ts_util:get_cols(),[DataRow]},
    {ok, Got} = ts_util:ts_get(
                  ts_util:cluster_and_connect(single),
                  normal, DDL, Data, Key, []),
    ?assertEqual(Expected, Got),
    pass.
