% -------------------------------------------------------------------
%%
%% Copyright (c) 2015, 2016 Basho Technologies, Inc.
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
-export([confirm/0, run_tests/2, run_tests/3]).
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
    inets:start(),
    run_tests(?PVAL_P1, ?PVAL_P2).

run_tests(PvalP1, PvalP2) ->
    run_tests(PvalP1, PvalP2, riakc_ts).

run_tests(PvalP1, PvalP2, ClientMod) ->
    Data = make_data(PvalP1, PvalP2),
    io:format("Data to be written:\n~p\n...\n~p\n", [hd(Data), lists:last(Data)]),

    [Node1|_] = Cluster = ts_util:build_cluster(single),

    %% 0. create table
    Client = get_client_at_node(Node1, ClientMod),
    io:format("using client mod ~s, client ~p\n", [ClientMod, Client]),
    ok = confirm_create_table({ClientMod, Client}),

    %% Make sure data is written to each node
    lists:foreach(
      fun(Node) ->
              confirm_all_from_node(
                {ClientMod, get_client_at_node(Node, ClientMod)},
                Data, PvalP1, PvalP2)
      end, Cluster),
    pass.

get_client_at_node(Node, ClientMod) ->
    case ClientMod of
        riakc_ts ->
            rt:pbc(Node);
        rhc_ts ->
            {ok, [{IP, Port}]} = rt:get_http_conn_info(Node),
            rhc_ts:create(IP, Port, [])
    end.

confirm_create_table({Mod, C}) ->
    TableDef =
        flat_format(
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
    ?assertEqual({[], []}, Mod:query(C, TableDef)),
    ok.

confirm_all_from_node(C, Data, PvalP1, PvalP2) ->

    %% 1. put some data
    ok = confirm_put(C, doctor_data(Data)),
    ok = confirm_put_nothing(C),
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

    %% Switch to native mode and repeat a few tests
    ok = switch_encoding_mode(C, true),

    %% 5 (redux). select
    ok = confirm_select(C, PvalP1, PvalP2),

    %% 6 (redux). single-key get some data
    ok = confirm_get(C, lists:nth(12, Data)),
    ok = confirm_nx_get(C),

    ok = switch_encoding_mode(C, false),

    ok = confirm_delete_all(C, RemainingKeys),
    {ok, []} = confirm_list_keys(C, 0).

switch_encoding_mode({riakc_ts, C}, Mode) ->
    riakc_pb_socket:use_native_encoding(C, Mode);
switch_encoding_mode(_, _) ->
    ok.

make_data(PvalP1, PvalP2) ->
    lists:reverse(
      lists:foldl(
        fun(T, Q) ->
                [[PvalP1,
                  PvalP2,
                  ?TIMEBASE + ?LIFESPAN - T + 1,
                  math:sin(float(T) / 100 * math:pi())] | Q]
        end,
        [], lists:seq(?LIFESPAN, 1, -1))).

doctor_data(Data) ->
    [[A, B, C, D + 0.42] || [A, B, C, D] <- Data].

confirm_put({Mod, C}, Data) ->
    ResFail = Mod:put(C, <<"no-bucket-like-this">>, Data),
    io:format("Nothing put in a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    %% Res = lists:map(fun(Datum) -> riakc_ts:put(C, ?BUCKET, [Datum]), timer:sleep(300) end, Data0),
    %% (was meant for tests of batch put writing order, obsoleted)
    Res = Mod:put(C, ?BUCKET, Data),
    io:format("Put ~b records: ~p\n", [length(Data), Res]),
    ?assertEqual(ok, Res),
    ok.

confirm_put_nothing({Mod, C}) ->
    Res = Mod:put(C, ?BUCKET, []),
    io:format("Nothing put in existing bucket: ~p\n", [Res]),
    ?assertMatch(ok, Res),
    ok.

confirm_overwrite({Mod, C}, Data) ->
    Res = Mod:put(C, ?BUCKET, Data),
    io:format("Overwrote ~b records: ~p\n", [length(Data), Res]),
    ?assertEqual(ok, Res),
    ok.

confirm_delete({Mod, C}, [Pooter1, Pooter2, Timepoint | _] = Record) ->
    ResFail = Mod:delete(C, <<"no-bucket-like-this">>, ?BADKEY, []),
    io:format("Nothing deleted from a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    Key = [Pooter1, Pooter2, Timepoint],

    BadKey1 = [Pooter1],
    BadRes1 = Mod:delete(C, ?BUCKET, BadKey1, []),
    io:format("Not deleted because short key: ~p\n", [BadRes1]),
    ?assertMatch({error, _}, BadRes1),

    BadKey2 = Key ++ [43],
    BadRes2 = Mod:delete(C, ?BUCKET, BadKey2, []),
    io:format("Not deleted because long key: ~p\n", [BadRes2]),
    ?assertMatch({error, _}, BadRes2),

    Res = Mod:delete(C, ?BUCKET, Key, []),
    io:format("Deleted record ~p: ~p\n", [Record, Res]),
    ?assertEqual(ok, Res),
    ok.

confirm_nx_delete({Mod, C}) ->
    Res = Mod:delete(C, ?BUCKET, ?BADKEY, []),
    io:format("Not deleted non-existing key: ~p\n", [Res]),
    case Mod of
        riakc_ts ->
            ?assertEqual(
               {error, {1021, <<"notfound">>}},
               Res);
        rhc_ts ->
            ?assertEqual(
               {error, notfound},
               Res)
    end,
    ok.

confirm_select({Mod, C}, PvalP1, PvalP2) ->
    Query =
        flat_format(
          "select score, pooter2 from ~s where"
          "     ~s = '~s'"
          " and ~s = '~s'"
          " and ~s > ~b and ~s < ~b",
          [?BUCKET,
           ?PKEY_P1, PvalP1,
           ?PKEY_P2, PvalP2,
           ?PKEY_P3, ?TIMEBASE + 10, ?PKEY_P3, ?TIMEBASE + 20]),
    io:format("Running query: ~p\n", [Query]),
    {_Columns, Rows} = Mod:query(C, Query),
    io:format("Got ~b rows back\n~p\n", [length(Rows), Rows]),
    ?assertEqual(10 - 1 - 1, length(Rows)),
    {_Columns, Rows} = Mod:query(C, Query),
    io:format("Got ~b rows back again\n", [length(Rows)]),
    ?assertEqual(10 - 1 - 1, length(Rows)),
    ok.

confirm_get({Mod, C}, Record = [Pooter1, Pooter2, Timepoint | _]) ->
    ResFail = Mod:get(C, <<"no-bucket-like-this">>, ?BADKEY, []),
    io:format("Got nothing from a non-existent bucket: ~p\n", [ResFail]),
    ?assertMatch({error, _}, ResFail),

    Key = [Pooter1, Pooter2, Timepoint],
    Res = Mod:get(C, ?BUCKET, Key, []),
    io:format("Get a single record: ~p\n", [Res]),
    ?assertMatch({ok, {_, [Record]}}, Res),
    ok.

confirm_nx_get({Mod, C}) ->
    Res = Mod:get(C, ?BUCKET, ?BADKEY, []),
    io:format("Not got a nonexistent single record: ~p\n", [Res]),
    ?assertMatch({error, notfound}, Res),
    ok.

confirm_nx_list_keys({_Mod, C}) ->
    {_Status, Reason} = Res = ts_list_keys(C, <<"no-bucket-like-this">>),
    io:format("Nothing listed from a non-existent bucket: ~p\n", [Reason]),
    ?assertMatch({error, notfound}, Res),
    ok.

confirm_list_keys(C, N) ->
    confirm_list_keys(C, N, 15).
confirm_list_keys(_C, _N, 0) ->
    io:format("stream_list_keys is lying\n", []),
    fail;
confirm_list_keys({_Mod, C} = CMod, N, TriesToGo) ->
    case ts_list_keys(C, ?BUCKET) of
        {ok, Keys} ->
            case length(Keys) of
                N ->
                    {ok, Keys};
                _NotN ->
                    io:format("Listed ~b (expected ~b) keys streaming. Retrying.\n", [length(Keys), N]),
                    confirm_list_keys(CMod, N, TriesToGo - 1)
            end;
        {error, Reason} ->
            io:format("streaming keys error: ~p\n", [Reason]),
            fail
    end.

confirm_delete_all({Mod, C}, AllKeys) ->
    io:format("Deleting ~b keys\n", [length(AllKeys)]),
    lists:foreach(
      fun(K) -> ok = Mod:delete(C, ?BUCKET, tuple_to_list(K), []) end,
      AllKeys),
    timer:sleep(3500),
    {ok, Res} = ts_list_keys(C, ?BUCKET),
    io:format("Deleted all: ~p\n", [Res]),
    ?assertMatch([], Res),
    ok.


%% ----------------------------

ts_list_keys(C, Table) when is_pid(C) ->
    %% there's only streaming version available for TS list_keys, so
    %% we slurp the stream here
    list_keys(C, Table);
ts_list_keys(C, Table) ->
    %% .. whereas rhc_ts does the slurping for us
    rhc_ts:list_keys(C, Table).

list_keys(C, Table) ->
    {ok, ReqId1} = riakc_ts:stream_list_keys(C, Table, []),
    receive_keys(ReqId1, []).

receive_keys(ReqId, Acc) ->
    receive
        {ReqId, {keys, Keys}} ->
            io:format("received batch of ~b\n", [length(Keys)]),
            receive_keys(ReqId, lists:append(Keys, Acc));
        {ReqId, {error, {1019, _BucketNotExistsMessage}}} ->
            {error, notfound};
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

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
