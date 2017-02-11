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
-export([query_invdist_percentile/1,
         query_invdist_percentile_backends/1,
         query_invdist_percentile_multiple/1,
         query_invdist_median/1,
         query_invdist_errors/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("ts_qbuf_util.hrl").


suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Cfg) ->
    Cluster = ts_setup:start_cluster(1),
    C = rt:pbc(hd(Cluster)),
    Data = make_data(),
    ok = create_table(C, ?TABLE),
    ok = insert_data(C, ?TABLE, Data),
    [{cluster, Cluster},
     {data, Data} | Cfg].

groups() ->
    [].

all() ->
    [
     query_invdist_percentile,
     query_invdist_percentile_multiple,
     query_invdist_percentile_backends,
     query_invdist_median,
     query_invdist_errors
    ].


%% test cases
%% -------------------------------

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
    ct:log("all inmem", []),
    WorkF(),
    load_intercept(Node, C, {riak_kv_qry_buffers, [{{can_afford_inmem, 1}, can_afford_inmem_no}]}),
    ct:log("all ldb", []),
    WorkF(),
    load_intercept(Node, C, {riak_kv_qry_buffers, [{{can_afford_inmem, 1}, can_afford_inmem_random}]}),
    ct:log("random", []),
    WorkF(),
    rt_intercept:clean(Node, riak_kv_qry_buffers),
    ok.


query_invdist_percentile_multiple(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    Data = proplists:get_value(data, Cfg),
    {Col1, Col2, Pc1, Pc2} = {"b", "b", 0.22, 0.77},
    Query = make_query(fmt("percentile(~s, ~g), percentile(~s, ~g)", [Col1, Pc1, Col2, Pc2])),
    {ok, {_Cols, [{Got1, Got2}]} = _Returned} =
        riakc_ts:query(C, Query, [], undefined, []),
    {Need1, Need2} = {get_percentile(Col1, Pc1, order_by(Col1, Data)),
                      get_percentile(Col2, Pc2, order_by(Col2, Data))},
    ct:log("Query \"~s\"", [Query]),
    case {Got1 == Need1, Got2 == Need2} of
        {true, true} ->
            ok;
        _ ->
            ct:fail("Got {~p, ~p}, Need {~p, ~p}\n", [Got1, Got2, Need1, Need2])
    end.

query_invdist_median(Cfg) ->
    C = rt:pbc(hd(proplists:get_value(cluster, Cfg))),
    lists:foreach(
      fun(Col) ->
              Query = make_query(fmt("percentile(~s, 0.5), median(~s)", [Col, Col])),
              {ok, {_Cols, [{Got1, Got2}]} = _Returned} =
                  riakc_ts:query(C, Query, [], undefined, []),
              ct:log("Query \"~s\"", [Query]),
              case Got1 == Got2 of
                  true ->
                      ok;
                  _ ->
                      ct:fail("Got {~p, ~p}, Need equal\n", [Got1, Got2])
              end
      end,
      ["a", "b", "c", "d", "e"]).

query_invdist_errors(Cfg) ->
    lists:foreach(
      fun({Select, Extra, ErrPat}) ->
              Qry = make_query(Select, Extra),
              ?assertEqual(
                 ok,
                 ts_qbuf_util:ack_query_error(Cfg, Qry, ?E_SUBMIT, ErrPat))
      end,
      [{"percentile(nxcol, 0.2)", [],
        "Unknown column \"nxcol\""},
       {"median(a), b", [],
        "Inverse distribution functions cannot be used with other columns in SELECT clause"},
       {"percentile(1)", [],
        "Function PERCENTILE/1 called with 1 argument"},
       {"percentile(a, 1.2)", [],
        "Invalid argument 2 in call to function PERCENTILE"},
       {"percentile(a, 0.1), percentile(b, 0.2)", [],
        "Multiple inverse distribution functions must all have the same column argument"},
       {"percentile(a, 1+c)", [],
        "Function 'PERCENTILE' called with arguments of the wrong type"},
       {"percentile(a, 1.1+c)", [],
        "Non-const expression passed as parameter for inverse distribution function"},
       {"percentile(a, 1.1/0)", [],
        "Invalid expression passed as parameter for inverse distribution function"},
       {"percentile(a, 0.3), avg(b)", [],
        "Inverse distribution functions cannot be used with GROUP BY clause or aggregating window functions"},
       {"percentile(a, 0.1)", "group by a",
        "Inverse distribution functions cannot be used with GROUP BY clause or aggregating window functions"},
       {"percentile(a, 0.1)", "order by a",
        "Inverse distribution functions cannot be used with any of ORDER BY, LIMIT or OFFSET clauses"},
       {"percentile(a, 0.1)", "limit 1",
        "Inverse distribution functions cannot be used with any of ORDER BY, LIMIT or OFFSET clauses"}]).


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
    make_query(Select, "").
make_query(Select, Extra) ->
    fmt("select ~s from ~s where a >= ~b and a <= ~b~s",
        [Select, ?TABLE,
         ?TIMEBASE, ?TIMEBASE * 1000,
         [" " ++ Extra || Extra /= []]]).



check_column(C, Col, Data) ->
    SortedData = order_by(Col, Data),
    ok == lists:foreach(
            fun({Pc, Pc_s}) ->
                    Query = make_query(fmt("percentile(~s, ~s)", [Col, Pc_s])),
                    {ok, {_Cols, [{Got}]} = _Returned} =
                        riakc_ts:query(C, Query, [], undefined, []),
                    Need = get_percentile(Col, Pc, SortedData),
                    ct:log("Query \"~s\"", [Query]),
                    case Got == Need of
                        true ->
                            ok;
                        false ->
                            ct:fail("Got ~p, Need ~p\n", [Got, Need])
                    end
            end,
            [{0.24, "0.24"},
             {0.11, "0.11"},
             {0.0, "0.0"},
             {0.8, "0.8*1"},
             {1.0, "1/1.0"},
             {0.35, "(3.5/10)"},
             {0.3, "0.4-(1*0.1)"}]).

order_by([FChar|_], Data) ->
    FNo = 1 + (FChar - $a),
    lists:sort(
      fun(Row1, Row2) ->
              element(FNo, Row1) =< element(FNo, Row2)
      end,
      Data).

get_percentile([FChar|_], Pc, Data) ->
    FNo = 1 + (FChar - $a),
    Pos = lists:min([1 + round(Pc * length(Data)), length(Data)]),
    element(FNo, lists:nth(Pos, Data)).


load_intercept(Node, C, Intercept) ->
    ok = rt_intercept:add(Node, Intercept),
    %% when code changes underneath the riak_kv_qry_buffers
    %% gen_server, it gets reinitialized. We need to probe it with a
    %% dummy query until it becomes ready.
    rt_intercept:wait_until_loaded(Node),
    wait_until_qbuf_mgr_reinit(C).

wait_until_qbuf_mgr_reinit(C) ->
    ProbingQuery = make_query("*", ""),
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
