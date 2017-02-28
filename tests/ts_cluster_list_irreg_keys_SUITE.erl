%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.
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
%% Tests for list_keys on DDLs having fields in LK and PK not in DDL order.
%%
%% -------------------------------------------------------------------
-module(ts_cluster_list_irreg_keys_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% We create several tables, each with the same number of fields [a,
%% b, c, ts, d], but with different participation of the first four in
%% the primary key. The atoms are mnemonics for the field order in the
%% key.
-define(ALL_VARIANTS, [abct, acbt, abtc, bat]).

-define(LIFESPAN, 250).  %% two and a half quanta

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    %% Allow listing of buckets and keys for testing
    application:set_env(riakc, allow_listing, true),

    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Cluster = ts_setup:start_cluster(1),
    [C1 | _] = [rt:pbc(Node) || Node <- Cluster],
    ok = create_tables(C1),
    Data = make_data(),
    ok = insert_data(C1, Data),
    [{cluster, Cluster}, {data, Data} | Config].

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
     list_key1_test,
     list_key2_test,
     list_key3_test,
     list_key4_test
    ].

%% -----------------------------
%% _test functions
%% -----------------------------

list_key1_test(Cfg) -> list_keysN(abct, Cfg).
list_key2_test(Cfg) -> list_keysN(acbt, Cfg).
list_key3_test(Cfg) -> list_keysN(abtc, Cfg).
list_key4_test(Cfg) -> list_keysN(bat,  Cfg).

list_keysN(Variant, Cfg) ->
    [Node|_] = ?config(cluster, Cfg),
    Client = rt:pbc(Node),
    Data = ?config(data, Cfg),
    {ok, GotKeysUnsorted} = list_keys(Client, make_table_name(Variant)),
    GotKeys = lists:sort(GotKeysUnsorted),
    ExpectKeys = lists:sort(make_keys(Variant, Data)),
    ?assertEqual(ExpectKeys, GotKeys).


%% ------------------------------
%% supporting functions
%% ------------------------------

list_keys(C, Bucket) ->
    {ok, ReqId1} = riakc_ts:stream_list_keys(C, Bucket, []),
    receive_keys(ReqId1, []).

receive_keys(ReqId, Acc) ->
    receive
        {ReqId, {keys, Keys}} ->
            receive_keys(ReqId, lists:append(Keys, Acc));
        {ReqId, {error, Reason}} ->
            {error, Reason};
        {ReqId, done} ->
            receive_keys(ReqId, Acc);
        _Else ->
            receive_keys(ReqId, Acc)
    after 3000 ->
            {ok, Acc}
    end.


create_tables(Client) ->
    lists:foreach(
      fun(Variant) ->
              Q = fmt("create table ~s ("
                      " a varchar not null,"
                      " b varchar not null,"
                      " c varchar not null,"
                      " ts timestamp not null,"
                      " d double,"
                      " ~s)", [make_table_name(Variant),
                               make_pk_definition(Variant)]),
              {ok, {[], []}} = riakc_ts:query(Client, Q, [])
      end,
      ?ALL_VARIANTS),
    ok.

insert_data(Client, Data) ->
    lists:foreach(
      fun(V) -> ok = riakc_ts:put(Client, list_to_binary(make_table_name(V)), Data) end,
      ?ALL_VARIANTS),
    ok.


make_data() ->
    lists:foldl(
      fun(T, Q) ->
              [{term_to_binary(T*10), <<"moo">>, iolist_to_binary("t"++integer_to_list(T)),
                T * 1000,
                math:sin(float(T) / 100 * math:pi())} | Q]
      end,
      [], lists:seq(1, ?LIFESPAN)).

make_keys(abct, Data) -> [{A, B, C, TS} || {A, B,  C, TS, _D} <- Data];
make_keys(acbt, Data) -> [{A, C, B, TS} || {A, B,  C, TS, _D} <- Data];
make_keys(abtc, Data) -> [{A, B, TS, C} || {A, B,  C, TS, _D} <- Data];
make_keys(bat,  Data) -> [{B, A,    TS} || {A, B, _C, TS, _D} <- Data].


%% "straight" key: field order is same in key and in DDL: this has always worked
make_pk_definition(abct) -> "primary key ((a, b, c, quantum(ts, 100, s)), a, b, c, ts)";
%% order is not the same
make_pk_definition(acbt) -> "primary key ((a, c, b, quantum(ts, 100, s)), a, c, b, ts)";
%% order is not the same, pk is subkey of lk
make_pk_definition(abtc) -> "primary key ((a, b,    quantum(ts, 100, s)), a, b, ts, c)";
%% order not the same, and one field is missing in the key
make_pk_definition(bat)  -> "primary key ((b, a,    quantum(ts, 100, s)), b, a,    ts)".

make_table_name(IAmAnAtom) -> atom_to_list(IAmAnAtom).

fmt(F, AA) ->
    lists:flatten(io_lib:format(F, AA)).
