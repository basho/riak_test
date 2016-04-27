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
    create_data_def_8(Pid),
    all_booleans_create_data(Pid),
    all_timestamps_create_data(Pid),
    all_types_create_data(Pid),
    quantum_first_table_create_data(Pid),
    quantum_first_table_three_fields_create_data(Pid),
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
%%% TABLE 1
%%%

create_data_def_1(Pid) ->
    ts_util:assert_row_sets({ok, {[],[]}},riakc_ts:query(Pid, table_def_1())),
    ok = riakc_ts:put(Pid, <<"table1">>, [{1,1,N,1} || N <- lists:seq(1,6000)]).

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
        {ok, {column_names_def_1(), Results}},
        run_query(Ctx, Query)
    ).

select_exclusive_def_1_2_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 44 AND c < 54",
    Results =
         [{1,1,N,1} || N <- lists:seq(45,53)],
    ts_util:assert_row_sets(
        {ok, {column_names_def_1(), Results}},
        run_query(Ctx, Query)
    ).

select_exclusive_def_1_across_quanta_1_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 500 AND c < 1500",
    Results =
         [{1,1,N,1} || N <- lists:seq(501,1499)],
    ts_util:assert_row_sets(
        {ok, {column_names_def_1(), Results}},
        run_query(Ctx, Query)
    ).

%% Across more quanta
select_exclusive_def_1_across_quanta_2_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 500 AND c < 4500",
    Results =
         [{1,1,N,1} || N <- lists:seq(501,4499)],
    ts_util:assert_row_sets(
        {ok, {column_names_def_1(), Results}},
        run_query(Ctx, Query)
    ).

select_inclusive_def_1_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 11 AND c <= 20",
    Results =
         [{1,1,N,1} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {ok, {column_names_def_1(), Results}},
        run_query(Ctx, Query)
    ).

%% Missing an a
where_clause_must_cover_the_partition_key_missing_a_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE b = 1 AND c > 0 AND c < 11",
    ?assertMatch(
        {error, {1001,<<_/binary>>}},
        run_query(Ctx, Query)
    ).

%% Missing a b
where_clause_must_cover_the_partition_key_missing_b_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1  AND c > 0 AND c < 11",
    ?assertMatch(
        {error, {1001,<<_/binary>>}},
        run_query(Ctx, Query)
    ).

%% Missing an c, the the quantum
where_clause_must_cover_the_partition_key_missing_c_test(Ctx) ->
    Query =
        "SELECT * FROM table1 WHERE b = 1",
    ?assertMatch(
        {error, {1001,<<_/binary>>}},
        run_query(Ctx, Query)
    ).

%%%
%%% TABLE 2 (same columns as table 1)
%%%

create_data_def_2(Pid) ->
    ts_util:assert_row_sets({ok, {[],[]}}, riakc_ts:query(Pid, table_def_2())),
    ok = riakc_ts:put(Pid, <<"table2">>, [{N,1,1,1} || N <- lists:seq(1,200)]).

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
        {ok, {column_names_def_1(), Results}},
        run_query(Ctx, Query)
    ).

select_inclusive_def_2_test(Ctx) ->
    Query =
        "SELECT * FROM table2 WHERE a >= 11 AND a <= 20",
    Results =
         [{N,1,1,1} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {ok, {column_names_def_1(), Results}},
        run_query(Ctx, Query)
    ).

%%%
%%% TABLE 3, small key where partition and local are the same
%%%

create_data_def_3(Pid) ->
    ts_util:assert_row_sets({ok, {[],[]}}, riakc_ts:query(Pid, table_def_3())),
    ok = riakc_ts:put(Pid, <<"table3">>, [{1,N} || N <- lists:seq(1,200)]).

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
        {ok, {column_names_def_3(), Results}},
        run_query(Ctx, Query)
    ).

select_inclusive_def_3_test(Ctx) ->
    Query =
        "SELECT * FROM table3 WHERE b >= 11 AND b <= 20 AND a = 1",
    Results =
         [{1,N} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {ok, {column_names_def_3(), Results}},
        run_query(Ctx, Query)
    ).


%%%
%%% TABLE 4, small key where partition and local are the same
%%%

create_data_def_4(Pid) ->
    ts_util:assert_row_sets({ok, {[],[]}}, riakc_ts:query(Pid, table_def_4())),
    ok = riakc_ts:put(Pid, <<"table4">>, [{1,1,N} || N <- lists:seq(1,200)]).

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
        {ok, {column_names_def_4(), Results}},
        run_query(Ctx, Query)
    ).

select_inclusive_def_4_test(Ctx) ->
    Query =
        "SELECT * FROM table4 WHERE a = 1 AND b = 1 AND c >= 11 AND c <= 20",
    Results =
         [{1,1,N} || N <- lists:seq(11,20)],
    ts_util:assert_row_sets(
        {ok, {column_names_def_4(), Results}},
        run_query(Ctx, Query)
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
    ts_util:assert_row_sets({ok, {[],[]}}, riakc_ts:query(Pid, table_def_5())),
    ok = riakc_ts:put(Pid, <<"table5">>, [{1,1,N} || N <- lists:seq(1,200)]).

select_def_5_test(Ctx) ->
    Query =
        "SELECT * FROM table5 WHERE a = 1 AND b = 1 AND c = 20",
    ts_util:assert_row_sets(
        {ok, {column_names_def_5(), [{1,1,20}]}},
        run_query(Ctx, Query)
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
    ts_util:assert_row_sets({ok, {[],[]}}, riakc_ts:query(Pid, table_def_6())),
    ok = riakc_ts:put(Pid, <<"table6">>, [{1,N,1,<<"table6">>} || N <- lists:seq(1,200)]).

select_def_6_test(Ctx) ->
    Query =
        "SELECT * FROM table6 WHERE b > 7 AND b < 14 AND a = 1 AND c = 1",
    Results =
         [{1,N,1,<<"table6">>} || N <- lists:seq(8,13)],
    ts_util:assert_row_sets(
        {ok, {[<<"a">>, <<"b">>, <<"c">>,<<"d">>], Results}},
        run_query(Ctx, Query)
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
    ts_util:assert_row_sets({ok, {[],[]}}, riakc_ts:query(Pid, table_def_7())),
    ok = riakc_ts:put(Pid, <<"table7">>, [{N,1,1,<<"table7">>} || N <- lists:seq(1,200)]).

select_exclusive_def_7_test(Ctx) ->
    Query =
        "SELECT * FROM table7 WHERE a > 44 AND a < 55 AND b = 1 AND c = 1",
    Results =
         [{N,1,1,<<"table7">>} || N <- lists:seq(45,54)],
    ts_util:assert_row_sets(
        {ok, {[<<"a">>, <<"b">>, <<"c">>, <<"d">>], Results}},
        run_query(Ctx, Query)
    ).

select_inclusive_def_7_test(Ctx) ->
    Query =
        "SELECT * FROM table7 WHERE a >= 44 AND a < 55 AND b = 1 AND c = 1",
    Results =
         [{N,1,1,<<"table7">>} || N <- lists:seq(44,54)],
    ts_util:assert_row_sets(
        {ok, {[<<"a">>, <<"b">>, <<"c">>, <<"d">>], Results}},
        run_query(Ctx, Query)
    ).

%%%
%%% Tests for where clause filters on additional fields in the local key.
%%%

create_data_def_8(Pid) ->
    ts_util:assert_row_sets(
        {ok, {[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE table8 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "d SINT64 NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c,d))"
    )),
    ok = riakc_ts:put(Pid, <<"table8">>, [{1,1,N,N} || N <- lists:seq(1,6000)]).

d_equal_than_filter_test(Ctx) ->
    Query =
        "SELECT * FROM table8 "
        "WHERE a = 1 AND b = 1 AND c >= 2500 AND c <= 4500 AND d = 3000",
    ts_util:assert_row_sets(
        {rt_ignore_columns, [{1,1,3000,3000}]},
        run_query(Ctx, Query)
    ).

d_greater_than_filter_test(Ctx) ->
    Query =
        "SELECT * FROM table8 "
        "WHERE a = 1 AND b = 1 AND c >= 2500 AND c <= 4500 AND d > 3000",
    Results =
         [{1,1,N,N} || N <- lists:seq(3001,4500)],
    ts_util:assert_row_sets(
        {rt_ignore_columns, Results},
        run_query(Ctx, Query)
    ).

d_greater_or_equal_to_filter_test(Ctx) ->
    Query =
        "SELECT * FROM table8 "
        "WHERE a = 1 AND b = 1 AND c >= 2500 AND c <= 4500 AND d >= 3000",
    Results =
         [{1,1,N,N} || N <- lists:seq(3000,4500)],
    ts_util:assert_row_sets(
        {rt_ignore_columns, Results},
        run_query(Ctx, Query)
    ).

d_not_filter_test(Ctx) ->
    Query =
        "SELECT * FROM table8 "
        "WHERE a = 1 AND b = 1 AND c >= 2500 AND c <= 4500 AND d != 3000",
    Results =
         [{1,1,N,N} || N <- lists:seq(2500,4500), N /= 3000],
    ts_util:assert_row_sets(
        {rt_ignore_columns, Results},
        run_query(Ctx, Query)
    ).

%%%
%%% ERROR CASE TESTS
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

duplicate_fields_in_local_key_1_not_allowed_test(Ctx) ->
    ?assertMatch(
        {error, {1020, <<_/binary>>}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE table1 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c,c))"
        )
    ).

duplicate_fields_in_local_key_2_not_allowed_test(Ctx) ->
    ?assertMatch(
        {error, {1020, <<_/binary>>}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE table1 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "d SINT64  NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c,d,d))"
        )
    ).

duplicate_fields_in_partition_key_1_not_allowed_test(Ctx) ->
    ?assertMatch(
        {error, {1020, <<_/binary>>}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE table1 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "d SINT64, "
            "PRIMARY KEY  ((a,a,quantum(c, 1, 's')), a,a,c))"
        )
    ).

multiple_quantum_functions_in_partition_key_not_allowed(Ctx) ->
    ?assertMatch(
        {error, {1020, <<_/binary>>}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE table1 ("
            "a SINT64 NOT NULL, "
            "b TIMESTAMP NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,quantum(b, 1, 's'),quantum(c, 1, 's')), a,b,c))"
        )
    ).

%%%
%%% Keys with different types
%%%

double_pk_double_boolean_lk_test(Ctx) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE double_pk_double_boolean_lk_test ("
            "a DOUBLE NOT NULL, "
            "b BOOLEAN NOT NULL, "
            "PRIMARY KEY  ((a), a,b))"
    )),
    Doubles = [N * 0.1 || N <- lists:seq(1,100)],
    ok = riakc_ts:put(client_pid(Ctx), <<"double_pk_double_boolean_lk_test">>,
        [{F,B} || F <- Doubles, B <- [true,false]]),
    Query =
        "SELECT * FROM double_pk_double_boolean_lk_test "
        "WHERE a = 0.5 AND b = true",
    ts_util:assert_row_sets(
        {rt_ignore_columns, [{0.5,true}]},
        run_query(Ctx, Query)
    ).

boolean_pk_boolean_double_lk_test(Ctx) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE boolean_pk_boolean_double_lk_test ("
            "a BOOLEAN NOT NULL, "
            "b DOUBLE NOT NULL, "
            "PRIMARY KEY  ((a), a,b))"
    )),
    Doubles = [N * 0.1 || N <- lists:seq(1,100)],
    ok = riakc_ts:put(client_pid(Ctx), <<"boolean_pk_boolean_double_lk_test">>,
        [{B,F} || F <- Doubles, B <- [true,false]]),
    Query =
        "SELECT * FROM boolean_pk_boolean_double_lk_test "
        "WHERE a = false AND b = 0.5",
    ts_util:assert_row_sets(
        {rt_ignore_columns, [{false,0.5}]},
        run_query(Ctx, Query)
    ).

all_types_create_data(Pid) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE all_types ("
            "a VARCHAR NOT NULL, "
            "b TIMESTAMP NOT NULL, "
            "c SINT64 NOT NULL, "
            "d BOOLEAN NOT NULL, "
            "e DOUBLE NOT NULL, "
            "f VARCHAR NOT NULL, "
            "g TIMESTAMP NOT NULL, "
            "h SINT64 NOT NULL, "
            "i BOOLEAN NOT NULL, "
            "j DOUBLE NOT NULL, "
            "k VARCHAR NOT NULL, "
            "l TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,b,c,d,e,f,g), a,b,c,d,e,f,g,h,i,j,k,l))"
    )),
    %% increasing `Num' increases the result set massivey
    Num = 3,
    Varchars = [<<"a">>,<<"b">>,<<"c">>],
    Timestamps = lists:seq(1,Num),
    Sint64s = lists:seq(1,Num),
    Booleans = ts_booleans(),
    Doubles = [N * 0.1 || N <- lists:seq(1,2)],
    %% hard code some of the local key values to reduce the result set
    H = 1,
    K = <<"k">>,
    L = 1,
    ok = riakc_ts:put(Pid, <<"all_types">>,
        [{A,B,C,D,E,F,G,H,I,J,K,L} || A <- Varchars,   B <- Timestamps,
                                      C <- Sint64s,    D <- Booleans,
                                      E <- Doubles,    F <- Varchars,
                                      G <- Timestamps,
                                      I <- Booleans,   J <- Doubles]).

all_types_1_test(Ctx) ->
    H = 1,
    K = <<"k">>,
    L = 1,
    Doubles = [N * 0.1 || N <- lists:seq(1,2)],
    Query =
        "SELECT * FROM all_types "
        "WHERE a = 'b' AND b = 1 AND c = 3 AND d = true AND e = 0.1 "
        "AND f = 'a' AND g = 2",
    Results =
        [{<<"b">>,1,3,true,0.1,<<"a">>,2,H,I,J,K,L} || 
            I <- ts_booleans()
           ,J <- Doubles
        ],
    ts_util:assert_row_sets(
        {rt_ignore_columns, Results},
        run_query(Ctx, Query)
    ).

all_types_or_filter_test(Ctx) ->
    H = 1,
    K = <<"k">>,
    L = 1,
    Doubles = [N * 0.1 || N <- lists:seq(1,2)],
    Query =
        "SELECT * FROM all_types "
        "WHERE a = 'b' AND b = 1 AND c = 3 AND d = true AND e = 0.1 "
        "AND f = 'a' AND g = 2 AND (i = true OR j = 0.2)",
    Results =
        [{<<"b">>,1,3,true,0.1,<<"a">>,2,H,I,J,K,L} || 
            I <- ts_booleans(),
            J <- Doubles,
            I == true orelse J == 0.2
        ],
    ts_util:assert_row_sets(
        {rt_ignore_columns, Results},
        run_query(Ctx, Query)
    ).

%%%
%%% Boolean Keys
%%%

all_booleans_create_data(Pid) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE all_booleans ("
            "a BOOLEAN NOT NULL, "
            "b BOOLEAN NOT NULL, "
            "c BOOLEAN NOT NULL, "
            "d BOOLEAN NOT NULL, "
            "e BOOLEAN NOT NULL, "
            "f BOOLEAN NOT NULL, "
            "g BOOLEAN NOT NULL, "
            "PRIMARY KEY  ((a,b,c), a,b,c,d,e,f,g))"
    )),
    ok = riakc_ts:put(Pid, <<"all_booleans">>,
        [{Ba,Bb,Bc,Bd,Be,Bf,Bg} || Ba <- ts_booleans(),
                                   Bb <- ts_booleans(), Bc <- ts_booleans(),
                                   Bd <- ts_booleans(), Be <- ts_booleans(),
                                   Bf <- ts_booleans(), Bg <- ts_booleans()]).

ts_booleans() ->
    [false,true]. %% false > true

all_booleans_test(Ctx) ->
    Query =
        "SELECT * FROM all_booleans "
        "WHERE a = true AND b = true AND c = true",
    Results =
        [{true,true,true,Bd,Be,Bf,Bg} || Bd <- ts_booleans(), Be <- ts_booleans(),
                                         Bf <- ts_booleans(), Bg <- ts_booleans()],
    ts_util:assert_row_sets(
        {rt_ignore_columns,Results},
        run_query(Ctx, Query)
    ).

all_booleans_filter_on_g_test(Ctx) ->
    Query =
        "SELECT * FROM all_booleans "
        "WHERE a = true AND b = true AND c = true AND g = false",
    Results =
        [{true,true,true,Bd,Be,Bf,false} || Bd <- ts_booleans(), Be <- ts_booleans(),
                                         Bf <- ts_booleans()],
    ts_util:assert_row_sets(
        {rt_ignore_columns,Results},
        run_query(Ctx, Query)
    ).

all_booleans_filter_on_d_and_f_test(Ctx) ->
    Query =
        "SELECT * FROM all_booleans "
        "WHERE a = true AND b = true AND c = true AND d = false AND f = true",
    Results =
        [{true,true,true,false,Be,true,Bg} || Be <- ts_booleans(), Bg <- ts_booleans()],
    ts_util:assert_row_sets(
        {rt_ignore_columns,Results},
        run_query(Ctx, Query)
    ).

%%%
%%% Time Stamp Keys
%%%

all_timestamps_create_data(Pid) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE all_timestamps ("
            "a TIMESTAMP NOT NULL, "
            "b TIMESTAMP NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "d TIMESTAMP NOT NULL, "
            "e TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,quantum(b,15,s),c), a,b,c,d,e))"
    )),
    ok = riakc_ts:put(Pid, <<"all_timestamps">>,
        [{A,B,3,4,5} || A <- [1,2,3], B <- lists:seq(100, 10000, 100)]).

all_timestamps_across_quanta_test(Ctx) ->
    Query =
        "SELECT * FROM all_timestamps "
        "WHERE a = 2 AND b > 200 AND b < 3000 AND c = 3",
    Results =
        [{2,B,3,4,5} || B <- lists:seq(300, 2900, 100)],
    ts_util:assert_row_sets(
        {rt_ignore_columns,Results},
        run_query(Ctx, Query)
    ).

all_timestamps_single_quanta_test(Ctx) ->
    Query =
        "SELECT * FROM all_timestamps "
        "WHERE a = 2 AND b > 200 AND b <= 900 AND c = 3",
    Results =
        [{2,B,3,4,5} || B <- lists:seq(300, 900, 100)],
    ts_util:assert_row_sets(
        {rt_ignore_columns,Results},
        run_query(Ctx, Query)
    ).

%%%
%%% Tables where the quantum is not last.
%%%

quantum_first_table_create_data(Pid) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE qf_table ("
            "a TIMESTAMP NOT NULL, "
            "b VARCHAR NOT NULL, "
            "PRIMARY KEY  ((quantum(a,1,s),b), a,b))"
    )),
    ok = riakc_ts:put(Pid, <<"qf_table">>,
        [{A,B} || A<- lists:seq(100, 10000, 100), B <- [<<"x">>, <<"y">>]]).

select_on_quantum_first_table_test(Ctx) ->
    Query =
        "SELECT * FROM qf_table "
        "WHERE  a > 200 AND a < 3000 AND b = 'x'",
    Results =
        [{A,<<"x">>} || A <- lists:seq(300, 2900, 100)],
    ts_util:assert_row_sets(
        {rt_ignore_columns,Results},
        run_query(Ctx, Query)
    ).

quantum_first_table_three_fields_create_data(Pid) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE qf_table2 ("
            "a TIMESTAMP NOT NULL, "
            "b SINT64 NOT NULL, "
            "c VARCHAR NOT NULL, "
            "PRIMARY KEY  ((quantum(a,1,s),b,c), a,b,c))"
    )),
    ok = riakc_ts:put(Pid, <<"qf_table2">>,
        [{A,B,C} || A <- lists:seq(100, 10000, 100), B <- [3,4,5], C <- [<<"x">>, <<"y">>]]).

select_on_quantum_first_table_three_fields_test(Ctx) ->
    Query =
        "SELECT * FROM qf_table2 "
        "WHERE  a > 200 AND a < 3000 AND b = 3 AND c = 'x'",
    Results =
        [{A,3,<<"x">>} || A <- lists:seq(300, 2900, 100)],
    ts_util:assert_row_sets(
        {rt_ignore_columns,Results},
        run_query(Ctx, Query)
    ).
