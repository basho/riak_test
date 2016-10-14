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

-module(ts_simple_query_buffers_ldb_errors).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(TABLE, "t1").
-define(TIMEBASE, (5*1000*1000)).
-define(LIFESPAN, 50).
-define(LIFESPAN_EXTRA, 51).
-define(WHERE_FILTER_A, <<"A00001">>).
-define(WHERE_FILTER_B, <<"B">>).

confirm() ->
    [Node] = Cluster = ts_util:build_cluster(single),
    Data = make_data(),
    Config = [{cluster, Cluster}, {data, Data}],

    C = rt:pbc(Node),
    ok = create_table(C, ?TABLE),
    ok = insert_data(C, ?TABLE,  Data),

    %% issue a query to create the dev1/data/.../query_buffers dir
    Query = full_query(?TABLE, [{order_by, [{"d", undefined, undefined}]}]),
    riakc_ts:query(C, Query, [], undefined, []),

    %% chmod the query buffer dir out of existence
    QBufDir = filename:join([rtdev:node_path(Node), "data/leveldb/query_buffers"]),
    Cmd = fmt("chmod -w '~s'", [QBufDir]),
    CmdOut = "" = os:cmd(Cmd),
    io:format("~s: '~s'", [Cmd, CmdOut]),

    ok = eleveldb_open_crash(Config),

    pass.

create_table(Client, Table) ->
    DDL = "
create table " ++ Table ++ "
(a varchar not null,
 b varchar not null,
 c timestamp not null,
 d varchar,
 e sint64,
 primary key ((a, b, quantum(c, 10, s)), a, b, c))",
    {ok, {[], []}} = riakc_ts:query(Client, DDL),
    ok.

data_generator(I) ->
    {list_to_binary(fmt("A~5..0b", [I rem 2])),
     <<"B">>,
     ?TIMEBASE + (I + 1) * 1000,
     list_to_binary(fmt("k~5..0b", [round(100 * math:sin(float(I) / 10 * math:pi()))])),
     if I rem 5 == 0 -> []; el/=se -> I end  %% sprinkle some NULLs
    }.

make_data() ->
    lists:map(
      fun data_generator/1, lists:seq(0, ?LIFESPAN)).

insert_data(C, Table, Data) ->
    ok = riakc_ts:put(C, Table, Data),
    ok.


eleveldb_open_crash(Cfg) ->
    Node = hd(proplists:get_value(cluster, Cfg)),
    C = rt:pbc(Node),
    Query = full_query(?TABLE, [{order_by, [{"d", undefined, undefined}]}]),
    Res = riakc_ts:query(C, Query, [], undefined, []),
    case Res of
        {error, {1023, ErrMsg}} ->  %% error codes defined in riak_kv_ts_svc.erl
            io:format("reported eleveldb:open error ~s", [ErrMsg]),
            ok;
        {error, OtherReason} ->
            io:format("eleveldb:open error not correctly reported: got ~p instead", [OtherReason]),
            fail;
        NonError ->
            io:format("eleveldb:open error not detected: got ~p instead", [NonError]),
            fail
    end.


base_query(Table) ->
    fmt("select * from ~s where a = '~s' and b = '~s' and c >= ~b and c <= ~b",
        [Table,
         binary_to_list(?WHERE_FILTER_A),
         binary_to_list(?WHERE_FILTER_B),
         ?TIMEBASE, ?TIMEBASE + ?LIFESPAN_EXTRA * 1000]).

full_query(Table, OptionalClauses) ->
    [OrderBy, Limit, Offset] =
        [proplists:get_value(Item, OptionalClauses) ||
            Item <- [order_by, limit, offset]],
    fmt("~s~s~s~s",
        [base_query(Table),
         [fmt(" order by ~s", [make_orderby_list(OrderBy)]) || OrderBy /= undefined],
         [fmt(" limit ~b", [Limit])                         || Limit   /= undefined],
         [fmt(" offset ~b", [Offset])                       || Offset  /= undefined]]).

make_orderby_list(EE) ->
    string:join(lists:map(fun make_orderby_with_qualifiers/1, EE), ", ").

make_orderby_with_qualifiers({F, Dir, Nulls}) ->
    fmt("~s~s~s", [F, [" "++Dir || Dir /= undefined], [" "++Nulls || Nulls /= undefined]]);
make_orderby_with_qualifiers({F, Dir}) ->
    make_orderby_with_qualifiers({F, Dir, undefined});
make_orderby_with_qualifiers(F) ->
    make_orderby_with_qualifiers({F, undefined, undefined}).

fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
