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
%% A simple timeseries test to write nulls to all various datatypes

-module(ts_A_put_all_null_datatypes).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(SPANNING_STEP, (1000)).

confirm() ->
    TestType = normal,
    DDL =
        "CREATE TABLE GeoCheckin ("
        " myfamily    varchar     not null,"
        " myseries    varchar     not null,"
        " time        timestamp   not null,"
        " myvarchar   varchar             ,"
        " myint       sint64              ,"
        " myfloat     double              ,"
        " mybool      boolean             ,"
        " mytimestamp timestamp           ,"
        " PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')),"
        " myfamily, myseries, time))",
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    N = 10,
    Data = make_data(N, Family, Series, []),
    Got = ts_util:ts_put(
            ts_util:cluster_and_connect(single), TestType, DDL, Data),
    ?assertEqual(ok, Got),
    pass.

make_data(0, _, _, Acc) ->
    Acc;
make_data(N, F, S, Acc) when is_integer(N) andalso N > 0 ->
    NewAcc = [
          F,
          S,
          1 + N * ?SPANNING_STEP,
          [],
          [],
          [],
          [],
          []
         ],
    make_data(N - 1, F, S, [NewAcc | Acc]).
