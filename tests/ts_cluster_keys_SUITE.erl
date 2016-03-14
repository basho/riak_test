%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%s
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Tests for the different combinations of keys supported by
%% Riak Time Series.
%%
%% -------------------------------------------------------------------
-module(ts_cluster_keys_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    [Node|_] = Cluster = ts_util:build_cluster(multiple),
    Pid = rt:pbc(Node),
    % create tables and populate them with data
    create_data_def_1(Pid),
    create_data_def_2(Pid),
    create_data_def_3(Pid),
    create_data_def_4(Pid),
    create_data_def_5(Pid),
    create_data_def_6(Pid),
    create_data_def_7(Pid),
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

%%%
%%% TABLE 1
%%%

create_data_def_1(Pid) ->
    ts_util:assert_row_sets({[],[]},riakc_ts:query(Pid, table_def_1())),
    ok = riakc_ts:put(Pid, <<"table1">>, [[1,1,N,1] || N <- lists:seq(1,200)]).

column_names_def_1() ->
    [<<"a">>, <<"b">>, <<"c">>, <<"d">>].

table_def_1() ->
    "CREATE TABLE table1 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "d SINT64 NOT NULL, "
    "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c,d))".

select_exclusive_def_1_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 0 AND c < 11",
    Results =
         [{1,1,N,1} || N <- lists:seq(1,10)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

select_exclusive_def_1_2_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 44 AND c < 54",
    Results =
         [{1,1,N,1} || N <- lists:seq(45,53)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

select_inclusive_def_1_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 11 AND c <= 20",
    Results =
         [{1,1,N,1} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

%%%
%%% TABLE 2 (same columns as table 1)
%%%

create_data_def_2(Pid) ->
    ts_util:assert_row_sets({[],[]}, riakc_ts:query(Pid, table_def_2())),
    ok = riakc_ts:put(Pid, <<"table2">>, [[N,1,1,1] || N <- lists:seq(1,200)]).

table_def_2() ->
    "CREATE TABLE table2 ("
    "a TIMESTAMP NOT NULL, "
    "b SINT64 NOT NULL, "
    "c SINT64 NOT NULL, "
    "d SINT64 NOT NULL, "
    "PRIMARY KEY  ((quantum(a, 1, 's')), a,b,c,d))".

select_exclusive_def_2_test(Ctx) ->
    Query =
        "SELECT * FROM table2 WHERE a > 0 AND a < 11",
    Results =
         [{N,1,1,1} || N <- lists:seq(1,10)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

select_inclusive_def_2_test(Ctx) ->
    Query =
        "SELECT * FROM table2 WHERE a >= 11 AND a <= 20",
    Results =
         [{N,1,1,1} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

%%%
%%% TABLE 3, small key where partition and local are the same
%%%

create_data_def_3(Pid) ->
    ts_util:assert_row_sets({[],[]}, riakc_ts:query(Pid, table_def_3())),
    ok = riakc_ts:put(Pid, <<"table3">>, [[1,N] || N <- lists:seq(1,200)]).

column_names_def_3() ->
    [<<"a">>, <<"b">>].

table_def_3() ->
    "CREATE TABLE table3 ("
    "a SINT64 NOT NULL, "
    "b TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a,quantum(b, 1, 's')),a,b))".

select_exclusive_def_3_test(Ctx) ->
    Query =
        "SELECT * FROM table3 WHERE b > 0 AND b < 11 AND a = 1",
    Results =
         [{1,N} || N <- lists:seq(1,10)],
    ts_util:assert_row_sets(
        {column_names_def_3(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

select_inclusive_def_3_test(Ctx) ->
    Query =
        "SELECT * FROM table3 WHERE b >= 11 AND b <= 20 AND a = 1",
    Results =
         [{1,N} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {column_names_def_3(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).


%%%
%%% TABLE 4, small key where partition and local are the same
%%%

create_data_def_4(Pid) ->
    ts_util:assert_row_sets({[],[]}, riakc_ts:query(Pid, table_def_4())),
    ok = riakc_ts:put(Pid, <<"table4">>, [[1,1,N] || N <- lists:seq(1,200)]).

column_names_def_4() ->
    [<<"a">>, <<"b">>, <<"c">>].

table_def_4() ->
    "CREATE TABLE table4 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a,b,quantum(c, 1, 's')),a,b,c))".

select_exclusive_def_4_test(Ctx) ->
    Query =
        "SELECT * FROM table4 WHERE a = 1 AND b = 1 AND c > 0 AND c < 11",
    Results =
         [{1,1,N} || N <- lists:seq(1,10)],
    ts_util:assert_row_sets(
        {column_names_def_4(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

select_inclusive_def_4_test(Ctx) ->
    Query =
        "SELECT * FROM table4 WHERE a = 1 AND b = 1 AND c >= 11 AND c <= 20",
    Results =
         [{1,1,N} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {column_names_def_4(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

%%%
%%% TABLE 5 no quanta
%%%

column_names_def_5() ->
    [<<"a">>, <<"b">>, <<"c">>].

table_def_5() ->
    "CREATE TABLE table5 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a,b,c),a,b,c))".

create_data_def_5(Pid) ->
    ts_util:assert_row_sets({[],[]}, riakc_ts:query(Pid, table_def_5())),
    ok = riakc_ts:put(Pid, <<"table5">>, [[1,1,N] || N <- lists:seq(1,200)]).

select_def_5_test(Ctx) ->
    Query =
        "SELECT * FROM table5 WHERE a = 1 AND b = 1 AND c = 20",
    ts_util:assert_row_sets(
        {column_names_def_5(), [{1,1,20}]},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

%%%
%%% TABLE 6 quantum is not the last key
%%%

table_def_6() ->
    "CREATE TABLE table6 ("
    "a SINT64 NOT NULL, "
    "b TIMESTAMP NOT NULL, "
    "c SINT64 NOT NULL, "
    "d VARCHAR NOT NULL, "
    "PRIMARY KEY ((a,quantum(b,1,'s'),c),a,b,c,d))".

create_data_def_6(Pid) ->
    ts_util:assert_row_sets({[],[]}, riakc_ts:query(Pid, table_def_6())),
    ok = riakc_ts:put(Pid, <<"table6">>, [[1,N,1,<<"table6">>] || N <- lists:seq(1,200)]).

select_def_6_test(Ctx) ->
    Query =
        "SELECT * FROM table6 WHERE b > 7 AND b < 14 AND a = 1 AND c = 1",
    Results =
         [{1,N,1,<<"table6">>} || N <- lists:seq(8,13)],
    ts_util:assert_row_sets(
        {[<<"a">>, <<"b">>, <<"c">>,<<"d">>], Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

%%%
%%% TABLE 7 quantum is the first key
%%%

table_def_7() ->
    "CREATE TABLE table7 ("
    "a TIMESTAMP NOT NULL, "
    "b SINT64 NOT NULL, "
    "c SINT64 NOT NULL, "
    "d VARCHAR NOT NULL, "
    "PRIMARY KEY ((quantum(a,1,'s'),b,c),a,b,c,d))".

create_data_def_7(Pid) ->
    ts_util:assert_row_sets({[],[]}, riakc_ts:query(Pid, table_def_7())),
    ok = riakc_ts:put(Pid, <<"table7">>, [[N,1,1,<<"table7">>] || N <- lists:seq(1,200)]).

select_exclusive_def_7_test(Ctx) ->
    Query =
        "SELECT * FROM table7 WHERE a > 44 AND a < 55 AND b = 1 AND c = 1",
    Results =
         [{N,1,1,<<"table7">>} || N <- lists:seq(45,54)],
    ts_util:assert_row_sets(
        {[<<"a">>, <<"b">>, <<"c">>, <<"d">>], Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

select_inclusive_def_7_test(Ctx) ->
    Query =
        "SELECT * FROM table7 WHERE a >= 44 AND a < 55 AND b = 1 AND c = 1",
    Results =
         [{N,1,1,<<"table7">>} || N <- lists:seq(44,54)],
    ts_util:assert_row_sets(
        {[<<"a">>, <<"b">>, <<"c">>, <<"d">>], Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

%%%
%%% NULL CHECKING TESTS
%%%

nulls_in_additional_local_key_not_allowed_test(Ctx) ->
    ?assertMatch(
        {error, {1020, <<_/binary>>}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE table1 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "d SINT64, "       %% d is in the local key and set as nullable
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c,d))"
        )
    ).


