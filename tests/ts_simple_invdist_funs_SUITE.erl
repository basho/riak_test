% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.
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
%% @doc Inverse distribution functions (PERCENTILE, MEDIAN)

-module(ts_simple_invdist_funs_SUITE).

-export([suite/0, init_per_suite/1, groups/0, all/0]).
-export([query_invdist_selftest/1,
         query_invdist_percentile/1,
         query_invdist_percentile_backends/1,
         query_invdist_percentile_multiple/1,
         query_invdist_median/1,
         query_invdist_mode/1,
         query_invdist_errors/1]).

-export([percentile_disc/3, percentile_cont/3]).  %% make them 'used' for erlc

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("ts_qbuf_util.hrl").

-define(TABLE_A, "table_with_regular_keys").
-define(TABLE_D, "table_with_descending_keys").

suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Cfg) ->
    Cluster = ts_setup:start_cluster(1),
    C = rt:pbc(hd(Cluster)),
    Data = make_data(),
    ok = create_table(C, ?TABLE_A),
    ok = create_table_desc(C, ?TABLE_D),
    ok = insert_data(C, ?TABLE_A, Data),
    ok = insert_data(C, ?TABLE_D, Data),
    [{cluster, Cluster},
     {data, Data} | Cfg].

groups() ->
    [].

all() ->
    [
     query_invdist_selftest,
     query_invdist_percentile,
     query_invdist_percentile_multiple,
     query_invdist_percentile_backends,
     query_invdist_median,
     query_invdist_mode,
     query_invdist_errors
    ].


%% test cases
%% -------------------------------

query_invdist_selftest(_Cfg) ->
    Data =
        [{X} || X <- lists:sort(pseudo_random_data())],
    {GotDisc, GotCont} =
        lists:unzip(
          [{round(100 * percentile_disc("a", Pc/100, Data)),
            round(100 * percentile_cont("a", Pc/100, Data))} || Pc <- lists:seq(1, 97)]),
    {NeedDisc, NeedCont} =
        lists:unzip(
          [{round(X * 100), round(Y * 100)} || {X, Y} <- externally_verified_percentiles()]),
    io:format("~p ~p\n", [length(GotDisc), length(NeedDisc)]),
    ?assertEqual(GotDisc, NeedDisc),
    ?assertEqual(GotCont, NeedCont).



query_invdist_percentile(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    Data = proplists:get_value(data, Cfg),
    lists:foreach(
      fun(Col) ->
              check_column(C, Col, Data) orelse
                  ct:fail("")
      end,
      ["a", "b", "c", "d", "e"]).


query_invdist_percentile_backends(Cfg) ->
    Node = hd(proplists:get_value(cluster, Cfg)),
    C = rt:pbc(Node),
    Data = proplists:get_value(data, Cfg),

    WorkF =
        fun() ->
                lists:foreach(
                  fun(Col) ->
                          check_column(C, Col, Data) orelse
                              ct:fail("")
                  end,
                  ["a", "b", "c", "d", "e"])
        end,

    rpc:call(Node, code, add_patha, [filename:join([rt_local:home_dir(), "../../ebin"])]),
    rt_intercept:load_code(Node),

    load_intercept(Node, C, {riak_kv_qry_buffers, [{{can_afford_inmem, 1}, can_afford_inmem_yes}]}),
    ct:log("----------- all inmem", []),
    WorkF(),
    load_intercept(Node, C, {riak_kv_qry_buffers, [{{can_afford_inmem, 1}, can_afford_inmem_no}]}),
    ct:log("----------- all ldb", []),
    WorkF(),
    load_intercept(Node, C, {riak_kv_qry_buffers, [{{can_afford_inmem, 1}, can_afford_inmem_random}]}),
    ct:log("----------- random", []),
    WorkF(),
    rt_intercept:clean(Node, riak_kv_qry_buffers),
    ok.


query_invdist_percentile_multiple(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    Data = proplists:get_value(data, Cfg),
    {Col1, Col2, Pc1, Pc2} = {"b", "b", 0.22, 0.77},
    Query = make_query(fmt("percentile_disc(~s, ~g), percentile_cont(~s, ~g)", [Col1, Pc1, Col2, Pc2])),
    {ok, {_Cols, [{Got1, Got2}]}} =
        riakc_ts:query(C, Query, [], undefined, []),
    {Need1, Need2} = {percentile_disc(Col1, Pc1, order_by(Col1, Data)),
                      percentile_cont(Col2, Pc2, order_by(Col2, Data))},
    ct:log("Query \"~s\"", [Query]),
    case {Got1 == Need1, Got2 == Need2} of
        {true, true} ->
            ok;
        _ ->
            ct:fail("PERCENTILE_DISC vs _CONT: Got {~p, ~p}, Need {~p, ~p}\n", [Got1, Got2, Need1, Need2])
    end.


query_invdist_median(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    lists:foreach(
      fun(Col) ->
              Query = make_query(fmt("percentile_disc(~s, 0.5), median(~s)", [Col, Col])),
              {ok, {_Cols, [{Got1, Got2}]}} =
                  riakc_ts:query(C, Query, [], undefined, []),
              ct:log("Query \"~s\"", [Query]),
              case Got1 == Got2 of
                  true ->
                      ok;
                  _ ->
                      ct:fail("MEDIAN: Got {~p, ~p}, Need equal\n", [Got1, Got2])
              end
      end,
      ["a", "b", "c", "d", "e"]).


query_invdist_mode(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    Data = proplists:get_value(data, Cfg),
    lists:foreach(
      fun(Col) ->
              SortedData = order_by(Col, Data),
              Query = make_query(fmt("mode(~s)", [Col])),
              {ok, {_Cols, [{Got}]}} =
                  riakc_ts:query(C, Query, [], undefined, []),
              ct:log("Query \"~s\"", [Query]),
              Need = mode(Col, SortedData),
              case Got == Need of
                  true ->
                      ok;
                  _ ->
                      ct:fail("MODE: Got ~p, Need ~p\n", [Got, Need])
              end
      end,
      ["a", "b", "c", "d", "e"]).


query_invdist_errors(Cfg) ->
    lists:foreach(
      fun({Select, Extra, ErrPat}) ->
              Qry = make_query(?TABLE_A, Select, Extra),
              ?assertEqual(
                 ok,
                 ts_qbuf_util:ack_query_error(Cfg, Qry, ?E_SUBMIT, ErrPat))
      end,
      [{"percentile_disc(nxcol, 0.2)", [],
        "Unknown column \"nxcol\""},
       {"median(a), b", [],
        "Inverse distribution functions .* cannot be used with other columns in SELECT clause"},
       {"percentile_disc(1)", [],
        "Function PERCENTILE_DISC/1 called with 1 argument"},
       {"percentile_disc(a, 1.2)", [],
        "Invalid argument 2 in call to function PERCENTILE_DISC"},
       {"percentile_disc(a, 0.1), percentile_disc(b, 0.2)", [],
        "Multiple inverse distribution functions .* must all have the same column argument"},
       {"percentile_disc(a, 1+c)", [],
        "Function 'PERCENTILE_DISC' called with arguments of the wrong type"},
       {"percentile_disc(a, 1.1+c)", [],
        "Inverse distribution functions .* must have a static const expression for its parameters"},
       {"percentile_disc(a, 1.1/0)", [],
        "Invalid expression passed as parameter for inverse distribution function"},
       {"percentile_disc(a, 0.3), avg(b)", [],
        "Inverse distribution functions .* cannot be used with GROUP BY clause or other aggregating window functions"},
       {"percentile_disc(a, 0.1)", "group by a",
        "Inverse distribution functions .* cannot be used with GROUP BY clause or other aggregating window functions"},
       {"percentile_disc(a, 0.1)", "order by a",
        "Inverse distribution functions .* cannot be used with any of ORDER BY, LIMIT or OFFSET clauses"},
       {"percentile_disc(a, 0.1)", "limit 1",
        "Inverse distribution functions .* cannot be used with any of ORDER BY, LIMIT or OFFSET clauses"}]).


%% tested functions implememnted independently
%% -------------------------------

percentile_disc(F, Pc, Data_) ->
    Col = remove_nulls(F, Data_),
    RN = (1 + (Pc * (length(Col) - 1))),
    lists:nth(trunc(RN), Col).

percentile_cont(F, Pc, Data_) ->
    Col = remove_nulls(F, Data_),
    RN = (1 + (Pc * (length(Col) - 1))),
    {LoRN, HiRN} = {trunc(RN), ceil(RN)},
    case LoRN == HiRN of
        true ->
            lists:nth(LoRN, Col);
        false ->
            LoVal = lists:nth(LoRN, Col),
            HiVal = lists:nth(HiRN, Col),
            (HiRN - RN) * LoVal + (RN - LoRN) * HiVal
    end.

ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

mode(F, Data_) ->
    Col = remove_nulls(F, Data_),
    Min = lists:nth(1, Col),
    largest_bin(Min, Col).

remove_nulls([FChar|_], Data_) ->
    FNo = 1 + (FChar - $a),
    [element(FNo, D) || D <- Data_, element(FNo, D) /= []].

largest_bin(X1, Data) ->
    largest_bin_({X1, 1, X1, 1}, 2, Data).

largest_bin_({LargestV, _, _, _}, Pos, Data) when Pos > length(Data) ->
    LargestV;
largest_bin_({LargestV, LargestC, CurrentV, CurrentC}, Pos, Data) ->
    case lists:nth(Pos, Data) of
        V when V == CurrentV ->
            largest_bin_({LargestV, LargestC,
                          CurrentV, CurrentC + 1}, Pos + 1, Data);
        V when V > CurrentV,
               CurrentC > LargestC ->
            largest_bin_({CurrentV, CurrentC,  %% now these be largest
                          V, 1}, Pos + 1, Data);
        V when V > CurrentV,
               CurrentC =< LargestC ->
            largest_bin_({LargestV, LargestC,  %% keep largest, reset current
                          V, 1}, Pos + 1, Data)
    end.


%% supporting functions
%% -------------------------------

create_table(Client, Table) ->
    DDL = "
create table " ++ Table ++ "
(a timestamp not null,
 b double,
 c sint64,
 d sint64,
 e sint64,
 primary key ((quantum(a, 10, h)), a))",
    {ok, {[], []}} = riakc_ts:query(Client, DDL),
    ok.

create_table_desc(Client, Table) ->
    DDL = "
create table " ++ Table ++ "
(a timestamp not null,
 b double,
 c sint64,
 d sint64,
 e sint64,
 primary key ((quantum(a, 10, h)), a desc))",
    {ok, {[], []}} = riakc_ts:query(Client, DDL),
    ok.

data_generator(I) ->
    {?TIMEBASE + (I + 1) * 1000,
     100 * math:cos(float(I) / 10 * math:pi()),
     case I rem 5 of 0 -> []; _ -> I end,  %% sprinkle some NULLs
     I+1,
     -I
    }.

make_data() ->
    [data_generator(I) || I <- lists:seq(0, ?LIFESPAN)].

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

make_query(Select) ->
    make_query(?TABLE_A, Select, "").
make_query(Table, Select) ->
    make_query(Table, Select, "").
make_query(Table, Select, Extra) ->
    fmt("select ~s from ~s where a >= ~b and a <= ~b~s",
        [Select, Table,
         ?TIMEBASE, ?TIMEBASE * 1000,
         [" " ++ Extra || Extra /= []]]).



check_column(C, Col, Data) ->
    SortedData = order_by(Col, Data),
    Combos =
        [{PercentileVariety, Parm, Parm_s} ||
            {Parm, Parm_s} <- [{0.24, "0.24"},
                               {0.11, "0.11"},
                               {0.0, "0.0"},
                               {0.8, "0.8*1"},
                               {1.0, "1/1.0"},
                               {0.36, "(3.6/10)"},
                               {0.40, "0.5 - 1 * 0.1"}],
            PercentileVariety <- [percentile_cont, percentile_disc]],
    Checker =
        fun(Q, Need) ->
                ct:log("Query \"~s\"", [Q]),
                {ok, {_Cols, [{Got}]}} =
                    riakc_ts:query(C, Q, [], undefined, []),
                case Got == Need of
                    true ->
                        ok;
                    false ->
                        ct:fail("Got ~p, Need ~p\n", [Got, Need])
                end
        end,
    ok == lists:foreach(
            fun({PercentileFun, Pc, Pc_s}) ->
                    Need = apply(?MODULE, PercentileFun, [Col, Pc, SortedData]),
                    ok = Checker(make_query(?TABLE_A, fmt("~s(~s, ~s)", [PercentileFun, Col, Pc_s])), Need),
                    ok = Checker(make_query(?TABLE_D, fmt("~s(~s, ~s)", [PercentileFun, Col, Pc_s])), Need)
            end,
            Combos).

order_by([FChar|_], Data) ->
    FNo = 1 + (FChar - $a),
    lists:sort(
      fun(Row1, Row2) ->
              element(FNo, Row1) =< element(FNo, Row2)
      end,
      Data).


%% intercepts

load_intercept(Node, C, Intercept) ->
    ok = rt_intercept:add(Node, Intercept),
    %% when code changes underneath the riak_kv_qry_buffers
    %% gen_server, it gets reinitialized. We need to probe it with a
    %% dummy query until it becomes ready.
    rt_intercept:wait_until_loaded(Node),
    wait_until_qbuf_mgr_reinit(C).

wait_until_qbuf_mgr_reinit(C) ->
    ProbingQuery = make_query("*"),
    case riakc_ts:query(C, ProbingQuery, [], undefined, []) of
        {ok, _Data} ->
            ok;
        {error, {ErrCode, _NotReadyMessage}}
          when ErrCode == ?E_QBUF_CREATE_ERROR;
               ErrCode == ?E_QBUF_INTERNAL_ERROR ->
            timer:sleep(100),
            wait_until_qbuf_mgr_reinit(C)
    end.


fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).

pseudo_random_data() ->
    [float(X) || X <- [1,2,3,4,5,7,0,11,-4,2,2,19,3,11,17,4,9]].

externally_verified_percentiles() ->
    %% these are generated with this script:
    %% #!/bin/env python
    %% import numpy as np
    %% a = np.array([1,2,3,4,5,7,0,11,-4,2,2,19,3,11,17,4,9])
    %% for p in range(1,99):
    %%     disc = np.percentile(a, p, interpolation='lower')
    %%     cont = np.percentile(a, p, interpolation='linear')
    %%     print("{0:.3f}, {1:.3f},".format(disc, cont))

    [{-4.000, -3.360},
     {-4.000, -2.720},
     {-4.000, -2.080},
     {-4.000, -1.440},
     {-4.000, -0.800},
     {-4.000, -0.160},
     {0.000, 0.120},
     {0.000, 0.280},
     {0.000, 0.440},
     {0.000, 0.600},
     {0.000, 0.760},
     {0.000, 0.920},
     {1.000, 1.080},
     {1.000, 1.240},
     {1.000, 1.400},
     {1.000, 1.560},
     {1.000, 1.720},
     {1.000, 1.880},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.000},
     {2.000, 2.120},
     {2.000, 2.280},
     {2.000, 2.440},
     {2.000, 2.600},
     {2.000, 2.760},
     {2.000, 2.920},
     {3.000, 3.000},
     {3.000, 3.000},
     {3.000, 3.000},
     {3.000, 3.000},
     {3.000, 3.000},
     {3.000, 3.000},
     {3.000, 3.040},
     {3.000, 3.200},
     {3.000, 3.360},
     {3.000, 3.520},
     {3.000, 3.680},
     {3.000, 3.840},
     {4.000, 4.000},
     {4.000, 4.000},
     {4.000, 4.000},
     {4.000, 4.000},
     {4.000, 4.000},
     {4.000, 4.000},
     {4.000, 4.000},
     {4.000, 4.120},
     {4.000, 4.280},
     {4.000, 4.440},
     {4.000, 4.600},
     {4.000, 4.760},
     {4.000, 4.920},
     {5.000, 5.160},
     {5.000, 5.480},
     {5.000, 5.800},
     {5.000, 6.120},
     {5.000, 6.440},
     {5.000, 6.760},
     {7.000, 7.080},
     {7.000, 7.400},
     {7.000, 7.720},
     {7.000, 8.040},
     {7.000, 8.360},
     {7.000, 8.680},
     {9.000, 9.000},
     {9.000, 9.320},
     {9.000, 9.640},
     {9.000, 9.960},
     {9.000, 10.280},
     {9.000, 10.600},
     {9.000, 10.920},
     {11.000, 11.000},
     {11.000, 11.000},
     {11.000, 11.000},
     {11.000, 11.000},
     {11.000, 11.000},
     {11.000, 11.000},
     {11.000, 11.480},
     {11.000, 12.440},
     {11.000, 13.400},
     {11.000, 14.360},
     {11.000, 15.320},
     {11.000, 16.280},
     {17.000, 17.080},
     {17.000, 17.400},
     {17.000, 17.720},
     {17.000, 18.040}
    ].
