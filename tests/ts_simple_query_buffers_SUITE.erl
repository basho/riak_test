% -------------------------------------------------------------------
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
%% @doc A module to test riak_ts queries with ORDER BY/LIMIT/OFFSET
%%      clauses, which use query buffers.  There are three test cases:
%%
%%      1. query_orderby_no_updates: check that, in an existing query
%%         buffer which has not expired, updates to the mother table
%%         do not get picked up by a follow-up query;

%%      2. query_orderby_expiring: conversely, check that updates do
%%         get picked up after query buffer expiry;

%%      3. query_orderby_comprehensive: test all combinations of up to
%%         5 ORDER BY elements, with and without NULLS FIRST/LAST and
%%         ASC/DESC modifiers.

%%      The DDL includes NULL fields, and data generated include
%%      duplicate fields as well as NULLs.

-module(ts_simple_query_buffers_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, "t1").
-define(TABLE2, "t2").  %% used to check for expiry; not to interfere with t1
-define(TIMEBASE, (5*1000*1000)).
-define(LIFESPAN, 100).
-define(LIFESPAN_EXTRA, 110).
-define(WHERE_FILTER_A, <<"A00001">>).
-define(WHERE_FILTER_B, <<"B">>).
-define(ORDBY_COLS, ["a", "b", "c", "d", "e", undefined]).


-export([suite/0, init_per_suite/1, groups/0, all/0]).
-export([query_orderby_comprehensive/1,
         query_orderby_no_updates/1,
         query_orderby_expiring/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
    Cluster = ts_util:build_cluster(single),
    [{cluster, Cluster},
     {data, make_data()},
     {extra_data, make_extra_data()}
     | Config].

groups() ->
    [].

all() ->
    [
     %% 2. check LIMIT and ORDER BY, not involving follow-up queries
     query_orderby_comprehensive,
     %% 1. check that query buffers persist and do not pick up updates to
     %% the mother table (and do, after expiry)
     query_orderby_no_updates,
     query_orderby_expiring
    ].


query_orderby_comprehensive(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    Data = proplists:get_value(data, Cfg),
    ok = create_table(C, ?TABLE),
    ok = insert_data(C, ?TABLE,  Data),
    ct:print("Testing all possible combinations of up to 5 elements in ORDER BY (can take some 2 min) ...", []),
    OrderByBattery1 =
        [[F1, F2, F3, F4, F5] ||
            F1 <- ?ORDBY_COLS,
            F2 <- ?ORDBY_COLS,
            F3 <- ?ORDBY_COLS,
            F4 <- ?ORDBY_COLS,
            F5 <- ?ORDBY_COLS,
            all_different([F1, F2, F3, F4, F5]),
            not_all_undefined([F1, F2, F3, F4, F5])],
    OrderByBattery2 =
        lists:map(
          fun(FF) ->
                  [F || F <- FF, F /= undefined]
          end,
          OrderByBattery1),
    lists:foreach(
      fun(Items) ->
              Variants =
                  make_ordby_item_variants(Items),
              lists:foreach(
                fun(Var) ->
                        check_sorted(C, ?TABLE, Data, [{order_by, Var}]),
                        check_sorted(C, ?TABLE, Data, [{order_by, Var}, {limit, 1}, {offset, 10}]),
                        check_sorted(C, ?TABLE, Data, [{order_by, Var}, {limit, 4}, {offset, 5}]),
                        check_sorted(C, ?TABLE, Data, [{order_by, Var}, {limit, 55}, {offset, 11}])
                end,
                Variants)
      end,
      OrderByBattery2).

all_different(XX) ->
    length(lists:usort(XX)) == length(XX).
not_all_undefined(XX) ->
    not lists:all(fun(X) -> X == undefined end, XX).

%% given a single list of fields, e.g., ["a", "b", "c"], produce all
%% variants featuring all possible modifiers with each field, thus:
%%  [[{"a", "asc", "nulls first"}, ...], ...,
%%   [{"a", undefined, undefined}, ...]]
make_ordby_item_variants(FF) ->
    Modifiers =
        [{Dir, Nul} || Dir <- ["asc", "desc", undefined],
                       Nul <- ["nulls first", "nulls last", undefined]],
    Unrolled =
        lists:append(
          [[{F, M1, M2}] || {M1, M2} <- Modifiers,
                            F <- FF]),
    rollup(length(FF), Unrolled, []).

rollup(_, [], Acc) ->
    Acc;
rollup(N, FF, Acc) ->
    {R, T} = lists:split(N, FF),
    rollup(N, T, [R | Acc]).


query_orderby_no_updates(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    Data = proplists:get_value(data, Cfg),
    ExtraData = proplists:get_value(extra_data, Cfg),
    ok = create_table(C, ?TABLE2),
    ok = insert_data(C, ?TABLE2, Data),
    check_sorted(C, ?TABLE2, Data, [{order_by, [{"d", undefined, undefined}]}], [{allow_qbuf_reuse, true}]),
    %% (a query buffer has been created)
    %% add a record
    ct:print("Adding extra data to see if query buffers do NOT pick it up", []),
    ok = insert_data(C, ?TABLE2, ExtraData),

    %% .. and check that the results don't include it
    check_sorted(C, ?TABLE2, Data,
                 [{order_by, [{"d", undefined, undefined}]}], [{allow_qbuf_reuse, true}]),

    %% a query with ONLY should have a query buffer of its own
    check_sorted(C, ?TABLE2, Data ++ ExtraData,
                 [{order_by, [{"d", undefined, undefined}]}], [{allow_qbuf_reuse, false}]),

    %% .. and check again, that the results still don't include the added record
    check_sorted(C, ?TABLE2, Data,
                 [{order_by, [{"d", undefined, undefined}]}, [{allow_qbuf_reuse, true}]]).


query_orderby_expiring(Cfg) ->
    Node = hd(proplists:get_value(cluster, Cfg)),
    C = rt:pbc(Node),
    Data = proplists:get_value(data, Cfg),
    ExtraData = proplists:get_value(extra_data, Cfg),

    %% (extra data have been added in query_orderby_no_updates)
    SQL_s = full_query(?TABLE2, [{order_by, [{"d", undefined, undefined}]}]),
    {select, SQL_q} =
        rpc:call(Node, riak_ql_parser, ql_parse,
                 [rpc:call(Node, riak_ql_lexer, get_tokens,
                           [SQL_s])]),
    {ok, SQL} =
        rpc:call(Node, riak_kv_ts_util, build_sql_record,
                 [select, SQL_q, []]),

    {ok, QBufRef} = rpc:call(Node, riak_kv_qry_buffers, make_qref, [SQL]),
    {ok, Expiry} = rpc:call(Node, riak_kv_qry_buffers, get_qbuf_expiry, [QBufRef]),
    ct:print("Sleeping for ~b msec until qbuf expires", [Expiry + 1100]),
    timer:sleep(Expiry + 1100),
    %% .. and check that the results does include it
    check_sorted(C, ?TABLE2, Data ++ ExtraData, [{order_by, [{"d", undefined, undefined}]}], [{allow_qbuf_reuse, true}]).


%% Common supporting functions


%% have columns named following this sequence: "a", "b", "c" ..., for
%% a shortcut relying on this to easily determine column number (see
%% col_no/1 below)
create_table(Client, Table) ->
    DDL = "\
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
     ?TIMEBASE + I + 1,
     list_to_binary(fmt("k~5..0b", [round(100 * math:sin(float(I) / 10 * math:pi()))])),
     if I rem 5 == 0 -> []; el/=se -> I end  %% sprinkle some NULLs
    }.

make_data() ->
    lists:map(
      fun data_generator/1, lists:seq(0, ?LIFESPAN)).
make_extra_data() ->
    lists:map(
      fun data_generator/1, lists:seq(?LIFESPAN, ?LIFESPAN_EXTRA)).

insert_data(C, Table, Data) ->
    ok = riakc_ts:put(C, Table, Data),
    ok.

base_query(Table) ->
    fmt("select * from ~s where a = '~s' and b = '~s' and c >= ~b and c <= ~b",
        [Table,
         binary_to_list(?WHERE_FILTER_A),
         binary_to_list(?WHERE_FILTER_B),
         ?TIMEBASE, ?TIMEBASE + ?LIFESPAN_EXTRA]).

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


check_sorted(C, Table, OrigData, Clauses) ->
    check_sorted(C, Table, OrigData, Clauses, [{allow_qbuf_reuse, true}]).
check_sorted(C, Table, OrigData, Clauses, Options) ->
    Query = full_query(Table, Clauses),
    {ok, {_Cols, Returned}} =
        riakc_ts:query(C, Query, [], undefined, Options),
    OrderBy = proplists:get_value(order_by, Clauses),
    Limit   = proplists:get_value(limit, Clauses),
    Offset  = proplists:get_value(offset, Clauses),
    %% emulate the SELECT on original data,
    %% including sorting according to ORDER BY
    %% uncomment these log entries to inspect the data visually (else
    %% it's just blows the log into tens of megs):
    %% ct:log("Query: \"~s\"", [Query]),
    %% ct:log("Fetched ~p", [Returned]),
    PreSelected = sort_by(
                    where_filter(OrigData, [{"a", ?WHERE_FILTER_A},
                                            {"b", ?WHERE_FILTER_B}]),
                    OrderBy),
    %% .. and including the LIMIT/OFFSET positions
    PreLimited =
        lists:sublist(PreSelected, safe_offset(Offset) + 1, safe_limit(Limit)),
    %% and finally, for comparison, truncate records to only fields by
    %% which sorting was done
    case truncate(Returned, OrderBy) ==
        truncate(PreLimited, OrderBy) of
        true ->
            ok;
        false ->
            ct:fail("Query ~s failed\nGot ~p\nNeed: ~p\n", [Query, Returned, PreLimited])
    end.

safe_offset(undefined) -> 0;
safe_offset(X) -> X.

safe_limit(undefined) -> ?LIFESPAN_EXTRA;
safe_limit(X) -> X.

truncate(RR, OrderBy) ->
    OrdByFieldPoses = [F - $a + 1 || {[F], _, _} <- OrderBy],
    lists:map(
      fun(Row) ->
              FF = tuple_to_list(Row),
              list_to_tuple(
                [lists:nth(Pos, FF) || Pos <- OrdByFieldPoses])
      end, RR).


where_filter(Rows, Filter) ->
    lists:filter(
      fun(Row) ->
              lists:all(
                fun({F, V}) ->
                        C = col_no(F),
                        element(C, Row) == V
                end,
                Filter)
      end,
      Rows).

%% emulate sorting of original data
sort_by(Data, OrderBy) ->
    OrdByDefined = default_orderby_modifiers(OrderBy),
    lists:sort(
      fun(R1, R2) ->
              LessThan =
                  lists:foldl(
                    fun(_, true) -> true;
                       (_, false) -> false;
                       ({Fld, Dir, Nul}, _) ->
                            C = col_no(Fld),
                            F1 = element(C, R1),
                            F2 = element(C, R2),
                            if F1 == F2 ->
                                    undefined;  %% let fields of lower precedence to decide
                               F1 == [] andalso F2 /= [] ->
                                    Nul == "nulls first";
                               F1 /= [] andalso F2 == [] ->
                                    Nul == "nulls last";
                               F1 < F2 ->
                                    (Dir == "asc");
                               F1 > F2 ->
                                    (Dir == "desc")
                            end
                    end,
                    undefined,
                    OrdByDefined),
              if LessThan == undefined ->
                      false;
                 el/=se ->
                      LessThan
              end
      end,
      Data).

default_orderby_modifiers(OrderBy) ->
    lists:map(
      %% "If NULLS LAST is specified, null values sort after all non-null
      %%  values; if NULLS FIRST is specified, null values sort before all
      %%  non-null values. If neither is specified, the default behavior is
      %%  NULLS LAST when ASC is specified or implied, and NULLS FIRST when
      %%  DESC is specified (thus, the default is to act as though nulls are
      %%  larger than non-nulls)."
      fun({F, "asc", undefined}) ->
              {F, "asc", "nulls last"};
         ({F, "desc", undefined}) ->
              {F, "desc", "nulls first"};
         ({F, Dir, Nul}) ->
              {F,
               if Dir == undefined -> "asc"; el/=se -> Dir end,
               if Nul == undefined -> "nulls last"; el/=se -> Nul end}
      end,
      OrderBy).

%% rely on "a" being column #1, "b" column #2, etc:
col_no([A|_]) ->
    A - $a + 1;
col_no({[A|_], _}) ->
    A - $a + 1;
col_no({[A|_], _, _}) ->
    A - $a + 1.

fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
