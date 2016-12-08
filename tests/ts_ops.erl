%% -*- Mode: Erlang -*-
%% -------------------------------------------------------------------
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
%% @doc A util module for riak_ts tests

-module(ts_ops).

-export([put/3,
         get/3, get/4,
         'query'/2, 'query'/3,
         insert/4, insert_no_columns/3
        ]).

-spec put(list(node()), string(), list(tuple())) -> 'ok'|{'error', term()}.
put(Nodes, Table, Records) ->
    riakc_ts:put(ts_setup:conn(Nodes), Table, Records).

get(Nodes, Table, Key) ->
    get(Nodes, Table, Key, []).

get(Nodes, Table, Key, Options) ->
    riakc_ts:get(ts_setup:conn(Nodes), Table, Key, Options).

'query'(Nodes, Qry) ->
    'query'(Nodes, Qry, []).

'query'(Nodes, Qry, Options) ->
    lager:info("Running query ~ts", [Qry]),
    Got = riakc_ts:query(ts_setup:conn(Nodes), Qry, [], undefined, Options),
    lager:info("Result is ~p", [Got]),
    Got.

insert_term_format(Data, Acc) when is_binary(Data) ->
    Acc ++ flat_format("'~s',", [Data]);
insert_term_format(Data, Acc) ->
    Acc ++ flat_format("~p,", [Data]).

insert(Nodes, Table, Columns, Data) ->
    Conn = ts_setup:conn(Nodes),
    ColFn = fun(Col, Acc) ->
        Acc ++ flat_format("~s,", [Col])
        end,
    TermFn = fun insert_term_format/2,
    ColClause = string:strip(lists:foldl(ColFn, [], Columns), right, $,),
    ValClause = string:strip(lists:foldl(TermFn, [], tuple_to_list(Data)), right, $,),
    SQL = flat_format("INSERT INTO ~s (~s) VALUES (~ts)",
                      [Table, ColClause, ValClause]),
    lager:info("~ts", [SQL]),
    Got = riakc_ts:query(Conn, SQL),
    lager:info("Result is ~p", [Got]),
    Got.

insert_no_columns(Nodes, Table, Data) ->
    Conn = ts_setup:conn(Nodes),
    TermFn = fun insert_term_format/2,
    ValClause = string:strip(lists:foldl(TermFn, [], tuple_to_list(Data)), right, $,),
    SQL = flat_format("INSERT INTO ~s VALUES (~ts)",
        [Table, ValClause]),
    lager:info("~ts", [SQL]),
    Got = riakc_ts:query(Conn, SQL),
    lager:info("Result is ~p", [Got]),
    Got.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
