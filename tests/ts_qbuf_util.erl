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
%% Common functions shared between ts_simple_query_buffers* modules

-module(ts_qbuf_util).

-export([
         ack_query_error/3,
         base_query/1,
         create_table/2,
         full_query/2,
         insert_data/3,
         make_data/0,
         make_extra_data/0
        ]).

-include("ts_qbuf_util.hrl").

%% have columns named following this sequence: "a", "b", "c" ..., for
%% a shortcut relying on this to easily determine column number (see
%% col_no/1 below)
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

make_extra_data() ->
    lists:map(
      fun data_generator/1, lists:seq(?LIFESPAN, ?LIFESPAN_EXTRA)).

insert_data(_C, _Table, []) ->
    ok;
insert_data(C, Table, Data) ->
    Batch = lists:sublist(Data, 50),
    ok = riakc_ts:put(C, Table, Batch),
    case catch lists:nthtail(50, Data) of
        Rest when is_list(Rest) ->
            insert_data(C, Table, Rest);
        _ ->
            ok
    end.


%% Form queries

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
         [fmt(" order by ~s", [make_orderby_list(OrderBy)]) || OrderBy /= []],
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


ack_query_error(Cfg, Query, ErrCode) ->
    Node = hd(proplists:get_value(cluster, Cfg)),
    C = rt:pbc(Node),
    Res = riakc_ts:query(C, Query, [], undefined, []),
    case Res of
        {error, {ErrCode, ErrMsg}} ->
            io:format("reported error ~s", [ErrMsg]),
            ok;
        {error, OtherReason} ->
            io:format("error not correctly reported: got ~p instead", [OtherReason]),
            fail;
        NonError ->
            io:format("error not detected: got ~p instead", [NonError]),
            fail
    end.


fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
