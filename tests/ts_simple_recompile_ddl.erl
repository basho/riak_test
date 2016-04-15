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

-module(ts_simple_recompile_ddl).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).
-define(LEGACY_TABLE, riak_kv_compile_tab).
-define(DETS_TABLE, riak_kv_compile_tab_v2).

confirm() ->
    {Cluster, _Conn} = ts_util:cluster_and_connect(single),
    Node = hd(Cluster),
    lists:foreach(
        fun(Table) ->
            DDL = create_table_sql(Table),
            ts_util:create_and_activate_bucket_type(Cluster, DDL, Table)
        end, test_tables()),
    rt:stop(Node),
    simulate_old_dets_entries(),
    rt:start(Node),
    rt:wait_until_nodes_ready(Cluster),
    verify_resulting_dets_entries(),
    pass.

open_dets(Table) ->
    FileDir = rtdev:riak_data(1),
    FilePath = filename:join(FileDir, [Table, ".dets"]),
    {ok, Table} = dets:open_file(Table, [{type, set}, {repair, force}, {file, FilePath}]).

simulate_old_dets_entries() ->
    open_dets(?DETS_TABLE),
    open_dets(?LEGACY_TABLE),
    Pid = spawn_link(fun() -> ok end),
    Pid2 = spawn_link(fun() -> ok end),
    Pid3 = spawn_link(fun() -> ok end),
    Table1DDL = sql_to_ddl(create_table_sql("Table1")),
    Table2DDL = sql_to_ddl(create_table_sql("Table2")),
    Table3DDL = sql_to_ddl(create_table_sql("Table3")),

    %% Here we want to test 3 degenerate cases:
    %% 1) An old DETS entry (pre-1.3) which does not a DDL compiler version
    %% 2) A compiled table with an older (pre-1.3) version
    %% 3) A table which seemingly was stuck in the compiling state
    ok = dets:insert(?LEGACY_TABLE, {<<"Table1">>, Table1DDL, Pid, compiled}),
    ok = dets:insert(?DETS_TABLE, {<<"Table2">>, 1, Table2DDL, Pid2, compiled}),
    ok = dets:insert(?DETS_TABLE, {<<"Table3">>, 1, Table3DDL, Pid3, compiling}),
    dets:close(?LEGACY_TABLE),
    dets:close(?DETS_TABLE).

verify_resulting_dets_entries() ->
    open_dets(?DETS_TABLE),
    open_dets(?LEGACY_TABLE),
    lager:debug("DETS =~p", [dets:match(?DETS_TABLE, {'$1', '$2','$3','$4','$5'})]),
    lists:foreach(fun(T) ->
        ?assertEqual(
            [[2, compiled]],
            dets:match(?DETS_TABLE, {T,'$1','_','_','$2'}))
        end, test_tables()),
    dets:close(?LEGACY_TABLE),
    dets:close(?DETS_TABLE).

test_tables() ->
    [<<"Table1">>,<<"Table2">>,<<"Table3">>].

create_table_sql(TableName) ->
    lists:flatten(io_lib:format("CREATE TABLE ~s ("
    " datum       varchar   not null,"
    " someseries  varchar   not null,"
    " time        timestamp not null,"
    " PRIMARY KEY ((datum, someseries, quantum(time, 15, 'm')), "
    " datum, someseries, time))", [TableName])).

sql_to_ddl(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, {DDL, _Props}} = riak_ql_parser:parse(Lexed),
    DDL.
