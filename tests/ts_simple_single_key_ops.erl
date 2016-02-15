-module(ts_simple_single_key_ops).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node1|_] = ts_util:build_cluster(single),
    Pid = rt:pbc(Node1),

    create_table_def_1(Pid),
    delete_single_key_def_1_test(Pid),

    create_table_def_2(Pid),
    delete_single_key_def_2_test(Pid),

    create_table_def_3(Pid),
    delete_single_key_def_3_test(Pid),

    pass.

%%%
%%% TABLE 1
%%%

table_def_1() ->
    "CREATE TABLE table1 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "d SINT64 NOT NULL, "
    "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))".

create_table_def_1(Pid) ->
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_1())),
    ok = riakc_ts:put(Pid, <<"table1">>, [[1,2,N,4] || N <- lists:seq(1,200)]).

delete_single_key_def_1_test(Pid) ->
    ?assertEqual(
        ok,
        riakc_ts:delete(Pid, <<"table1">>, [1,2,4], [])
    ).

%%%
%%% TABLE 2
%%%

table_def_2() ->
    "CREATE TABLE table2 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "d SINT64 NOT NULL, "
    "PRIMARY KEY  ((d,a,quantum(c, 1, 's')), d,a,c))".

create_table_def_2(Pid) ->
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_2())),
    ok = riakc_ts:put(Pid, <<"table2">>, [[1,2,N,4] || N <- lists:seq(1,200)]).

delete_single_key_def_2_test(Pid) ->
    ?assertEqual(
        ok,
        riakc_ts:delete(Pid, <<"table2">>, [4,1,10], [])
    ).

%%%
%%% TABLE 3
%%%

table_def_3() ->
    "CREATE TABLE table3 ("
    "ax SINT64 NOT NULL, "
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "d SINT64 NOT NULL, "
    "PRIMARY KEY  ((ax,a,quantum(c, 1, 's')), ax,a,c))".

create_table_def_3(Pid) ->
    ?assertEqual({[],[]}, riakc_ts:query(Pid, table_def_3())),
    ok = riakc_ts:put(Pid, <<"table3">>, [[1,2,3,N,4] || N <- lists:seq(1,200)]).

delete_single_key_def_3_test(Pid) ->
    ?assertEqual(
        ok,
        riakc_ts:delete(Pid, <<"table3">>, [1,2,20], [])
    ).