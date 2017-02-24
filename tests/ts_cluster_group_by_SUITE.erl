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
%% Tests for range queries around the boundaries of quanta.
%%
%% -------------------------------------------------------------------
-module(ts_cluster_group_by_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    [_Node|_] = Cluster = ts_setup:start_cluster(3),
    [{cluster, Cluster} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() ->
    rt:grep_test_functions(?MODULE).

client_pid(Ctx) ->
    [Node|_] = proplists:get_value(cluster, Ctx),
    rt:pbc(Node).

run_query(Ctx, Query) ->
    riakc_ts:query(client_pid(Ctx), Query).

%%%
%%% TESTS
%%%

select_grouped_field_test(Ctx) ->
    Table = "grouptab1",
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE grouptab1 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))"
    )),
    ok = riakc_ts:put(client_pid(Ctx), Table,
        [{1,B,C} || B <- [1,2,3], C <- [1,2,3]]),
    Query =
        "SELECT c FROM " ++ Table ++ " "
        "WHERE a = 1 AND b = 1 AND c >= 1 AND c <= 1000 "
        "GROUP BY c",
    {ok, {Cols, Rows}} = run_query(Ctx, Query),
    ts_data:assert_row_sets(
        {rt_ignore_columns, [{1},{2},{3}]},
        {ok,{Cols, lists:sort(Rows)}}
    ).

group_by_2_test(Ctx) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE grouptab2 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "d SINT64 NOT NULL, "
            "e SINT64 NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c,d))"
    )),
    ok = riakc_ts:put(client_pid(Ctx), <<"grouptab2">>,
        [{1,1,CE,D,CE} || CE <- lists:seq(1,1000), D <- [1,2,3]]),
    Query =
        "SELECT d, AVG(e) FROM grouptab2 "
        "WHERE a = 1 AND b = 1 AND c >= 1 AND c <= 1000 "
        "GROUP BY d",
    {ok, {Cols, Rows}} = run_query(Ctx, Query),
    ts_data:assert_row_sets(
        {rt_ignore_columns, [{1,500.5},{2,500.5},{3,500.5}]},
        {ok,{Cols, lists:sort(Rows)}}
    ).

group_by_time_test(Ctx) ->
    ?assertMatch(
        {ok, _},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE grouptab3 ("
            "a TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((quantum(a,1,s)), a))"
    )),
    ok = riakc_ts:put(client_pid(Ctx), <<"grouptab3">>,
        [{A} || A <- lists:seq(1,10000,2)]),
    Query =
        "SELECT time(a,1s), COUNT(*) FROM grouptab3 "
        "WHERE a >= 1 AND a <= 10000"
        "GROUP BY time(a,1s)",
    {ok, {Cols, Rows}} = run_query(Ctx, Query),
    ts_data:assert_row_sets(
        {rt_ignore_columns, [{N*1000,500} || N <- lists:seq(0,9)]},
        {ok,{Cols, lists:sort(Rows)}}
    ).
