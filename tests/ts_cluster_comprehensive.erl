% -------------------------------------------------------------------
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
%% @doc A module to test riak_ts basic create bucket/put/select cycle,
%% including testing native Erlang term_to_binary encoding.

-module(ts_cluster_comprehensive).
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
-define(LIFESPAN, 300).  %% > 100, which is the default chunk size for list_keys

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
    ?assertEqual({ok, {[], []}}, riakc_ts:query(rt:pbc(hd(Cluster)), TableDef)),

    %% Make sure data is written to each node
    lists:foreach(fun(Node) -> confirm_all_from_node(Node, Data, PvalP1, PvalP2) end, Cluster),
    pass.


confirm_all_from_node(Node, Data, PvalP1, PvalP2) ->
    %% set up a client
    C = rt:pbc(Node),

    %% 1. put some data
    ok = confirm_put(C, doctor_data(Data)),
    ok = confirm_overwrite(C, Data),

    %% 2. get a single key
    ok = confirm_get(C, lists:nth(12, Data)),
    ok = confirm_nx_get(C),

    %% 3. list keys and delete one
    ok = confirm_nx_list_keys(C),
    {ok, First} = confirm_list_keys(C, ?LIFESPAN),
    io:format("Before delete = ~p", [length(First)]),

    ok = confirm_delete(C, lists:nth(14, Data)),
    ok = confirm_nx_delete(C),

    %% Pause briefly. Deletions have a default 3 second
    %% reaping interval, and our list keys test may run
    %% afoul of that.
    timer:sleep(3500),
    {ok, RemainingKeys} = confirm_list_keys(C, ?LIFESPAN - 1),

    %% 5. select
    ok = confirm_select(C, PvalP1, PvalP2),

    %% 6. single-key get some data
    ok = confirm_get(C, lists:nth(12, Data)),
    ok = confirm_nx_get(C),

    %% Switch to protocol buffer mode and repeat a few tests

    %% 5 (redux). select
    ok = confirm_select(C, PvalP1, PvalP2, [{use_ttb, false}]),

    %% 6 (redux). single-key get some data
    ok = confirm_get(C, lists:nth(12, Data), [{use_ttb, false}]),
    ok = confirm_nx_get(C, [{use_ttb, false}]),

    ok = confirm_delete_all(C, RemainingKeys),
    {ok, []} = confirm_list_keys(C, 0).


make_data(PvalP1, PvalP2) ->
    lists:reverse(
      lists:foldl(
        fun(T, Q) ->
                [{PvalP1,
                  PvalP2,
                  ?TIMEBASE + ?LIFESPAN - T + 1,
                  math:sin(float(T) / 100 * math:pi())} | Q]
        end,
        [], lists:seq(?LIFESPAN, 1, -1))).

doctor_data(Data) ->
    [[A, B, C, D + 0.42] || [A, B, C, D] <- Data].

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

confirm_overwrite(C, Data) ->
    Res = riakc_ts:put(C, ?BUCKET, Data),
    io:format("Overwrote ~b records: ~p\n", [length(Data), Res]),
    ?assertEqual(ok, Res),
    ok.

confirm_delete(C, {Pooter1, Pooter2, Timepoint, _} = Record) ->
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
    ?assertEqual(
        {error, {1021, <<"notfound">>}},
        riakc_ts:delete(C, ?BUCKET, ?BADKEY, [])
    ),
    ok.

confirm_select(C, PvalP1, PvalP2) ->
    confirm_select(C, PvalP1, PvalP2, []).
confirm_select(C, PvalP1, PvalP2, Options) ->
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
    {ok, {_Columns, Rows}} = riakc_ts:query(C, Query, Options),
    io:format("Got ~b rows back\n~p\n", [length(Rows), Rows]),
    ?assertEqual(10 - 1 - 1, length(Rows)),
    {ok, {_Columns, Rows}} = riakc_ts:query(C, Query, Options),
    io:format("Got ~b rows back again\n", [length(Rows)]),
    ?assertEqual(10 - 1 - 1, length(Rows)),
    ok.

confirm_get(C, Record) ->
    confirm_get(C, Record, []).
confirm_get(C, Record = {Pooter1, Pooter2, Timepoint, _}, Options) ->
    ResFail = riakc_ts:get(C, <<"no-bucket-like-this">>, ?BADKEY, []),
    io:format("Got nothing from a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    Key = [Pooter1, Pooter2, Timepoint],
    Res = riakc_ts:get(C, ?BUCKET, Key, Options),
    io:format("Get a single record: ~p\n", [Res]),
    ?assertMatch({ok, {_, [Record]}}, Res),
    ok.

confirm_nx_get(C) ->
    confirm_nx_get(C, []).
confirm_nx_get(C, Options) ->
    Res = riakc_ts:get(C, ?BUCKET, ?BADKEY, Options),
    io:format("Not got a nonexistent single record: ~p\n", [Res]),
    ?assertMatch({ok, {[], []}}, Res),
    ok.

confirm_nx_list_keys(C) ->
    {Status, Reason} = list_keys(C, <<"no-bucket-like-this">>),
    io:format("Nothing listed from a non-existent bucket: ~p\n", [Reason]),
    ?assertMatch(error, Status),
    ok.

confirm_list_keys(C, N) ->
    confirm_list_keys(C, N, 15).
confirm_list_keys(_C, _N, 0) ->
    io:format("stream_list_keys is lying\n", []),
    fail;
confirm_list_keys(C, N, TriesToGo) ->
    {Status, Keys} = list_keys(C, ?BUCKET),
    case length(Keys) of
        N ->
            {ok, Keys};
        _NotN ->
            io:format("Listed ~b (expected ~b) keys streaming (~p). Retrying.\n", [length(Keys), N, Status]),
            confirm_list_keys(C, N, TriesToGo - 1)
    end.

confirm_delete_all(C, AllKeys) ->
    io:format("Deleting ~b keys\n", [length(AllKeys)]),
    lists:foreach(
      fun(K) -> ok = riakc_ts:delete(C, ?BUCKET, tuple_to_list(K), []) end,
      AllKeys),
    timer:sleep(3500),
    {ok, Res} = list_keys(C, ?BUCKET),
    io:format("Deleted all: ~p\n", [Res]),
    ?assertMatch([], Res),
    ok.

list_keys(C, Bucket) ->
    {ok, ReqId1} = riakc_ts:stream_list_keys(C, Bucket, []),
    receive_keys(ReqId1, []).

receive_keys(ReqId, Acc) ->
    receive
        {ReqId, {keys, Keys}} ->
            io:format("received batch of ~b\n", [length(Keys)]),
            receive_keys(ReqId, lists:append(Keys, Acc));
        {ReqId, {error, Reason}} ->
            io:format("list_keys(~p) at ~b got an error: ~p\n", [ReqId, length(Acc), Reason]),
            {error, Reason};
        {ReqId, done} ->
            io:format("done receiving from one quantum\n", []),
            receive_keys(ReqId, Acc);
        Else ->
            io:format("What's that? ~p\n", [Else]),
            receive_keys(ReqId, Acc)
    after 3000 ->
            io:format("Consider streaming done\n", []),
            {ok, Acc}
    end.
