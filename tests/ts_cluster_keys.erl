-module(ts_cluster_keys).

-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node1|_] = ts_util:build_cluster(multiple),
    Pid = rt:pbc(Node1),

    %% table setup and population
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_1())),
    ok = riakc_ts:put(Pid, <<"table1">>, [[1,1,N,1] || N <- lists:seq(1,200)]),
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_2())),
    ok = riakc_ts:put(Pid, <<"table2">>, [[N,1,1,1] || N <- lists:seq(1,200)]),
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_3())),
    ok = riakc_ts:put(Pid, <<"table3">>, [[1,N] || N <- lists:seq(1,200)]),
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_4())),
    ok = riakc_ts:put(Pid, <<"table4">>, [[1,1,N] || N <- lists:seq(1,200)]),

    %% actual tests
    select_exclusive_def_1_test(Pid),
    select_inclusive_def_1_test(Pid),

    select_exclusive_def_2_test(Pid),
    select_inclusive_def_2_test(Pid),

    select_exclusive_def_3_test(Pid),
    select_inclusive_def_3_test(Pid),

    select_exclusive_def_4_test(Pid),
    select_inclusive_def_4_test(Pid),

    pass.

%%%
%%% TABLE 1
%%%

column_names_def_1() ->
    [<<"a">>, <<"b">>, <<"c">>, <<"d">>].

table_def_1() ->
    "CREATE TABLE table1 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "d SINT64 NOT NULL, "
    "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c,d))".

select_exclusive_def_1_test(Pid) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c > 0 AND c < 11",
    Results =
         [{1,1,N,1} || N <- lists:seq(1,10)],
    ?assertEqual(
        {column_names_def_1(), Results},
        riakc_ts:query(Pid, Query)
    ).

select_inclusive_def_1_test(Pid) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 11 AND c <= 20",
    Results =
         [{1,1,N,1} || N <- lists:seq(11,20)],
    ?assertEqual(
        {column_names_def_1(), Results},
        riakc_ts:query(Pid, Query)
    ).

%%%
%%% TABLE 2 (same columns as table 1)
%%%

table_def_2() ->
    "CREATE TABLE table2 ("
    "a TIMESTAMP NOT NULL, "
    "b SINT64 NOT NULL, "
    "c SINT64 NOT NULL, "
    "d SINT64 NOT NULL, "
    "PRIMARY KEY  ((quantum(a, 1, 's')), a,b,c,d))".

select_exclusive_def_2_test(Pid) ->
    Query =
        "SELECT * FROM table2 WHERE a > 0 AND a < 11",
    Results =
         [{N,1,1,1} || N <- lists:seq(1,10)],
    ?assertEqual(
        {column_names_def_1(), Results},
        riakc_ts:query(Pid, Query)
    ).

select_inclusive_def_2_test(Pid) ->
    Query =
        "SELECT * FROM table2 WHERE a >= 11 AND a <= 20",
    Results =
         [{N,1,1,1} || N <- lists:seq(11,20)],
    ?assertEqual(
        {column_names_def_1(), Results},
        riakc_ts:query(Pid, Query)
    ).

%%%
%%% TABLE 3, small key where partition and local are the same
%%%

column_names_def_3() ->
    [<<"a">>, <<"b">>].

table_def_3() ->
    "CREATE TABLE table3 ("
    "a SINT64 NOT NULL, "
    "b TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a,quantum(b, 1, 's')),a,b))".

select_exclusive_def_3_test(Pid) ->
    Query =
        "SELECT * FROM table3 WHERE b > 0 AND b < 11 AND a = 1",
    Results =
         [{1,N} || N <- lists:seq(1,10)],
    ?assertEqual(
        {column_names_def_3(), Results},
        riakc_ts:query(Pid, Query)
    ).

select_inclusive_def_3_test(Pid) ->
    Query =
        "SELECT * FROM table3 WHERE b >= 11 AND b <= 20 AND a = 1",
    Results =
         [{1,N} || N <- lists:seq(11,20)],
    ?assertEqual(
        {column_names_def_3(), Results},
        riakc_ts:query(Pid, Query)
    ).


%%%
%%% TABLE 4, small key where partition and local are the same
%%%

column_names_def_4() ->
    [<<"a">>, <<"b">>, <<"c">>].

table_def_4() ->
    "CREATE TABLE table4 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a,b,quantum(c, 1, 's')),a,b,c))".

select_exclusive_def_4_test(Pid) ->
    Query =
        "SELECT * FROM table4 WHERE a = 1 AND b = 1 AND c > 0 AND c < 11",
    Results =
         [{1,1,N} || N <- lists:seq(1,10)],
    ?assertEqual(
        {column_names_def_4(), Results},
        riakc_ts:query(Pid, Query)
    ).

select_inclusive_def_4_test(Pid) ->
    Query =
        "SELECT * FROM table4 WHERE a = 1 AND b = 1 AND c >= 11 AND c <= 20",
    Results =
         [{1,1,N} || N <- lists:seq(11,20)],
    ?assertEqual(
        {column_names_def_4(), Results},
        riakc_ts:query(Pid, Query)
    ).
