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
%% Tests for range queries around the boundaries of quanta.
%%
%% -------------------------------------------------------------------
-module(ts_cluster_quantum_boundaries_SUITE).

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
    create_data_def_1(rt:pbc(Node)),
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
    [
     start_key_query_greater_than_1999_test
    ,start_key_query_greater_than_2000_test
    ,start_key_query_greater_than_2001_test
    ,start_key_query_greater_or_equal_to_1999_test
    ,start_key_query_greater_or_equal_to_2000_test
    ,start_key_query_greater_or_equal_to_2001_test
    ,end_key_query_less_than_3999_test
    ,end_key_query_less_than_4000_test
    ,end_key_query_less_than_4001_test
    ,end_key_query_less_than_or_equal_to_3999_test
    ,end_key_query_less_than_or_equal_to_4000_test
    ,end_key_query_less_than_or_equal_to_4001_test
    ].

%%%
%%% TABLE 1 local key one element longer than partition key
%%%

create_data_def_1(Pid) ->
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_1())),
    ok = riakc_ts:put(Pid, <<"table1">>, [[1,1,N] || N <- lists:seq(1,10000)]).

column_names_def_1() ->
    [<<"a">>, <<"b">>, <<"c">>].

table_def_1() ->
    "CREATE TABLE table1 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))".

client_pid(Ctx) ->
    [Node|_] = proplists:get_value(cluster, Ctx),
    rt:pbc(Node).

%%%
%%% START KEY TESTS
%%%

start_key_query_greater_than_1999_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 1999 AND c <= 3800",
    Results =
         [{1,1,N} || N <- lists:seq(2000,3800)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

start_key_query_greater_than_2000_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 2000 AND c <= 3800",
    Results =
         [{1,1,N} || N <- lists:seq(2001,3800)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

start_key_query_greater_than_2001_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 2001 AND c <= 3800",
    Results =
         [{1,1,N} || N <- lists:seq(2002,3800)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

start_key_query_greater_or_equal_to_1999_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 1999 AND c <= 3800",
    Results =
         [{1,1,N} || N <- lists:seq(1999,3800)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

start_key_query_greater_or_equal_to_2000_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 2000 AND c <= 3800",
    Results =
         [{1,1,N} || N <- lists:seq(2000,3800)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

start_key_query_greater_or_equal_to_2001_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 2001 AND c <= 3800",
    Results =
         [{1,1,N} || N <- lists:seq(2002,3800)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

%%%
%%% END KEY TESTS
%%%

end_key_query_less_than_3999_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 2500 AND c < 3999",
    Results =
         [{1,1,N} || N <- lists:seq(2500,3998)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

end_key_query_less_than_4000_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 2500 AND c < 4000",
    Results =
         [{1,1,N} || N <- lists:seq(2500,3999)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

end_key_query_less_than_4001_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 2500 AND c < 4001",
    Results =
         [{1,1,N} || N <- lists:seq(2500,4000)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

end_key_query_less_than_or_equal_to_3999_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 2500 AND c <= 3999",
    Results =
         [{1,1,N} || N <- lists:seq(2500,3999)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

end_key_query_less_than_or_equal_to_4000_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 2500 AND c <= 4000",
    Results =
         [{1,1,N} || N <- lists:seq(2500,4000)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).

end_key_query_less_than_or_equal_to_4001_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 2500 AND c <= 4001",
    Results =
         [{1,1,N} || N <- lists:seq(2500,4001)],
    ts_util:assert_row_sets(
        {column_names_def_1(), Results},
        riakc_ts:query(client_pid(Ctx), Query)
    ).