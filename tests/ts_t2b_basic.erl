%% -*- Mode: Erlang -*-
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
%% @doc A module to test riak_ts basic create bucket/put/select cycle using
%% native Erlang term_to_binary encoding.

-module(ts_t2b_basic).
-behavior(riak_test).
-export([confirm/0, run_tests/2]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"ts_test_bucket_one">>).
-define(PKEY_P1, <<"pooter1">>).
-define(PKEY_P2, <<"pooter2">>).
-define(PKEY_P3, <<"time">>).
-define(PVAL_P1, <<"ZXC11">>).
-define(PVAL_P2, <<"PDP-11">>).
-define(TIMEBASE, (10*1000*1000)).
-define(BADKEY, [<<"b">>,<<"a">>, ?TIMEBASE-1]).

confirm() ->
    run_tests(?PVAL_P1, ?PVAL_P2).

run_tests(PvalP1, PvalP2) ->
    Data = make_data(PvalP1, PvalP2),
    io:format("Data to be written:\n~p\n...\n~p\n", [hd(Data), lists:last(Data)]),

    Cluster = ts_util:build_cluster(multiple),

    %% use riak-admin to create a bucket
    TableDef = io_lib:format(
                 "CREATE TABLE ~s "
                 "(~s varchar not null, "
                 " ~s varchar not null, "
                 " ~s timestamp not null, "
                 " score double not null, "
                 " PRIMARY KEY ((~s, ~s, quantum(~s, 10, s)), ~s, ~s, ~s))",
                 [?BUCKET,
                  ?PKEY_P1, ?PKEY_P2, ?PKEY_P3,
                  ?PKEY_P1, ?PKEY_P2, ?PKEY_P3,
                  ?PKEY_P1, ?PKEY_P2, ?PKEY_P3]),
    ts_util:create_and_activate_bucket_type(Cluster, lists:flatten(TableDef), ?BUCKET),

    %% Make sure data is written to each node
    lists:foreach(fun(Node) -> confirm_all_from_node(Node, Data, PvalP1, PvalP2) end, Cluster),
    pass.


confirm_all_from_node(Node, Data, PvalP1, PvalP2) ->
    %% set up a client
    C = rt:pbc(Node),
    riakc_pb_socket:use_native_encoding(C, true),

    %% 1. put some data
    ok = confirm_put(C, Data),

    %% 4. delete one
    ok = confirm_delete(C, lists:nth(15, Data)),
    ok = confirm_nx_delete(C),

    %% 5. select
    ok = confirm_select(C, PvalP1, PvalP2),

    %% 6. single-key get some data
    ok = confirm_get(C, lists:nth(12, Data)),
    ok = confirm_nx_get(C),

    ok = confirm_list_keys(C),
    ok = confirm_delete_all(C).


-define(LIFESPAN, 300).  %% > 100, which is the default chunk size for list_keys
make_data(PvalP1, PvalP2) ->
    lists:reverse(
      lists:foldl(
        fun(T, Q) ->
                [[PvalP1,
                  PvalP2,
                  ?TIMEBASE + ?LIFESPAN - T + 1,
                  math:sin(float(T) / 100 * math:pi())] | Q]
        end,
        [], lists:seq(?LIFESPAN, 0, -1))).

confirm_put(C, Data) ->
    ResFail = riakc_ts:put(C, <<"no-bucket-like-this">>, Data),
    io:format("Nothing put in a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    %% Res = lists:map(fun(Datum) -> riakc_ts:put(C, ?BUCKET, [Datum]), timer:sleep(300) end, Data0),
    %% (for future tests of batch put writing order)
    Res = riakc_ts:put(C, ?BUCKET, Data),
    io:format("Put ~b records: ~p\n", [length(Data), Res]),
    ?assertEqual(ok, Res),
    ok.

confirm_delete(C, [Pooter1, Pooter2, Timepoint | _] = Record) ->
    ResFail = riakc_ts:delete(C, <<"no-bucket-like-this">>, ?BADKEY, []),
    io:format("Nothing deleted from a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    Key = [Pooter1, Pooter2, Timepoint],

    BadKey1 = [Pooter1],
    BadRes1 = riakc_ts:delete(C, ?BUCKET, BadKey1, []),
    io:format("Not deleted because short key: ~p\n", [BadRes1]),
    ?assertMatch({error, _}, BadRes1),

    BadKey2 = Key ++ [43],
    BadRes2 = riakc_ts:delete(C, ?BUCKET, BadKey2, []),
    io:format("Not deleted because long key: ~p\n", [BadRes2]),
    ?assertMatch({error, _}, BadRes2),

    Res = riakc_ts:delete(C, ?BUCKET, Key, []),
    io:format("Deleted record ~p: ~p\n", [Record, Res]),
    ?assertEqual(ok, Res),
    ok.

confirm_nx_delete(C) ->
    Res = riakc_ts:delete(C, ?BUCKET, ?BADKEY, []),
    io:format("Not deleted non-existing key: ~p\n", [Res]),
    ?assertEqual(ok, Res),
    ok.

confirm_select(C, PvalP1, PvalP2) ->
    Query =
        lists:flatten(
          io_lib:format(
            "select score, pooter2 from ~s where"
            "     ~s = '~s'"
            " and ~s = '~s'"
            " and ~s > ~b and ~s < ~b",
           [?BUCKET,
            ?PKEY_P1, PvalP1,
            ?PKEY_P2, PvalP2,
            ?PKEY_P3, ?TIMEBASE + 10, ?PKEY_P3, ?TIMEBASE + 20])),
    io:format("Running query: ~p\n", [Query]),
    {_Columns, Rows} = riakc_ts:query(C, Query),
    io:format("Got ~b rows back\n~p\n", [length(Rows), Rows]),
    ?assertEqual( 10 - 1 - 1, length(Rows)),
    {_Columns, Rows} = riakc_ts:query(C, Query),
    io:format("Got ~b rows back again\n", [length(Rows)]),
    ?assertEqual(10 - 1 - 1, length(Rows)),
    ok.

confirm_get(C, Record = [Pooter1, Pooter2, Timepoint | _]) ->
    ResFail = riakc_ts:get(C, <<"no-bucket-like-this">>, ?BADKEY, []),
    io:format("Got nothing from a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    Key = [Pooter1, Pooter2, Timepoint],
    Res = riakc_ts:get(C, ?BUCKET, Key, []),
    io:format("Get a single record: ~p\n", [Res]),
    ?assertMatch({ok, {_, [Record]}}, Res),
    ok.

confirm_nx_get(C) ->
    Res = riakc_ts:get(C, ?BUCKET, ?BADKEY, []),
    io:format("Not got a nonexistent single record: ~p\n", [Res]),
    ?assertMatch({ok, {[], []}}, Res),
    ok.

confirm_list_keys(C) ->
    ResFail = riakc_ts:list_keys(C, <<"no-bucket-like-this">>, []),
    io:format("Nothing listed from a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    {keys, Keys} = _Res = riakc_ts:list_keys(C, ?BUCKET, []),
    io:format("Listed ~b keys\n", [length(Keys)]),
    ?assertEqual(?LIFESPAN, length(Keys)),
    ok.

confirm_delete_all(C) ->
    {keys, Keys} = riakc_ts:list_keys(C, ?BUCKET, []),
    lists:foreach(
      fun(K) -> ok = riakc_ts:delete(C, ?BUCKET, tuple_to_list(K), []) end,
      Keys),
    {keys, Res} = riakc_ts:list_keys(C, ?BUCKET, []),
    io:format("Deleted all: ~p\n", [Res]),
    ?assertMatch([], Res),
    ok.
