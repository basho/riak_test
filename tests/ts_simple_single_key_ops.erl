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

-module(ts_simple_single_key_ops).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node1|_] = ts_setup:start_cluster(1),
    Pid = rt:pbc(Node1),

    create_table_def_1(Pid),
    delete_single_key_def_1_test(Pid),

    create_table_def_2(Pid),
    delete_single_key_def_2_test(Pid),

    create_table_def_3(Pid),
    delete_single_key_def_3_test(Pid),

    create_table_def_4(Pid),
    query_key_after_it_has_been_deleted_test(Pid),
    query_key_in_range_after_it_has_been_deleted_test(Pid),

    create_data_table_def_desc_on_quantum_table(Pid),
    delete_single_key_desc_on_quantum_test(Pid),

    create_data_table_def_desc_on_varchar_table(Pid),
    delete_single_key_desc_on_varchar_test(Pid),
    get_single_key_desc_on_varchar_test(Pid),
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
    ?assertEqual({ok, {[],[]}}, riakc_ts:query(Pid, table_def_1())),
    ok = riakc_ts:put(Pid, <<"table1">>, [{1,2,N,4} || N <- lists:seq(1,200)]).

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
    ?assertEqual({ok, {[],[]}}, riakc_ts:query(Pid, table_def_2())),
    ok = riakc_ts:put(Pid, <<"table2">>, [{1,2,N,4} || N <- lists:seq(1,200)]).

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
    ?assertEqual({ok, {[],[]}}, riakc_ts:query(Pid, table_def_3())),
    ok = riakc_ts:put(Pid, <<"table3">>, [{1,2,3,N,4} || N <- lists:seq(1,200)]).

delete_single_key_def_3_test(Pid) ->
    ?assertEqual(
        ok,
        riakc_ts:delete(Pid, <<"table3">>, [1,2,20], [])
    ).

%%%
%%% TABLE 4
%%%

create_table_def_4(Pid) ->
    ?assertEqual({ok, {[],[]}}, riakc_ts:query(Pid, 
        "CREATE TABLE table4 ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))")),
    ok = riakc_ts:put(Pid, <<"table4">>, [{1,2,N} || N <- lists:seq(1,50)]).

%% query just the key that has been deleted
query_key_after_it_has_been_deleted_test(Pid) ->
    riakc_ts:delete(Pid, <<"table4">>, [1,2,4], []),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid, "SELECT * FROM table4 WHERE a = 1 AND b = 2 AND c >= 4 AND c <= 4", [])
    ).

%% delete another key and make sure that it does not get returned in the results
query_key_in_range_after_it_has_been_deleted_test(Pid) ->
    riakc_ts:delete(Pid, <<"table4">>, [1,2,15], []),
    ?assertEqual(
        {ok, {[<<"a">>, <<"b">>, <<"c">>],[{1,2,N} || N <- lists:seq(11,19), N /= 15]}},
        riakc_ts:query(Pid, "SELECT * FROM table4 WHERE a = 1 AND b = 2 AND c > 10 AND c < 20", [])
    ).
%%%
%%% DESC ON QUANTUM TABLE
%%%

create_data_table_def_desc_on_quantum_table(Pid) ->
    ?assertEqual({ok, {[],[]}}, riakc_ts:query(Pid,
        "CREATE TABLE desc_on_quantum_table ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c DESC))")),
    ok = riakc_ts:put(Pid, <<"desc_on_quantum_table">>, [{1,2,N} || N <- lists:seq(1,200)]).

delete_single_key_desc_on_quantum_test(Pid) ->
    ?assertEqual(
        ok,
        riakc_ts:delete(Pid, <<"desc_on_quantum_table">>, [1,2,20], [])
    ).

%%%
%%% DESC ON VARCHAR TABLE
%%%

create_data_table_def_desc_on_varchar_table(Pid) ->
    ?assertEqual({ok, {[],[]}}, riakc_ts:query(Pid,
        "CREATE TABLE desc_on_varchar_table ("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b DESC,c))")),
    ok = riakc_ts:put(Pid, <<"desc_on_varchar_table">>, [{1,<<N:8>>,N} || N <- lists:seq(1,50)]).

delete_single_key_desc_on_varchar_test(Pid) ->
    ?assertEqual(
        ok,
        riakc_ts:delete(Pid, <<"desc_on_varchar_table">>, [1,<<2:8>>,2], [])
    ).

get_single_key_desc_on_varchar_test(Pid) ->
    ?assertEqual(
        {ok,{[<<"a">>,<<"b">>,<<"c">>],[{1,<<5>>,5}]}},
        riakc_ts:get(Pid, <<"desc_on_varchar_table">>, [1,<<5:8>>,5], [])
    ).
