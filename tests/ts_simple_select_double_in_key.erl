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

-module(ts_simple_select_double_in_key).
-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

%%
confirm() ->
    TestType = normal,
    TableDef =
        "CREATE TABLE GeoCheckin ("
        " myfamily    double    not null,"
        " myseries    varchar   not null,"
        " time        timestamp not null,"
        " PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')), "
        " myfamily, myseries, time))",
    Query =
        "SELECT * FROM GeoCheckin "
        "WHERE time >= 1 AND time <= 10 "
        "AND myseries = 'series' "
        "AND myfamily = 13.777744543543500002342342342342342340000000017777445435435000023423423423423423400000000177774454354350000234234234234234234000000001",
    ?assertEqual(
        {ok, {[<<"myfamily">>, <<"myseries">>, <<"time">>], input_data()}},
        ts_util:ts_query(
            ts_util:cluster_and_connect(single), TestType, TableDef, input_data(), Query)),
    pass.

%%
input_data() ->
    Times = lists:seq(1, 10),
    [{13.777744543543500002342342342342342340000000017777445435435000023423423423423423400000000177774454354350000234234234234234234000000001, <<"series">>, T} || T <- Times].

