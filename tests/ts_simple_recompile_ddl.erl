%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2106 Basho Technologies, Inc.
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
-define(PREVIOUS_TABLE, riak_kv_compile_tab_v2).
-define(CURRENT_TABLE, riak_kv_compile_tab_v3).

confirm() ->
    [Node | _Rest] = Cluster = ts_setup:start_cluster(1),
    lists:foreach(
        fun(Table) ->
            DDL = create_table_sql(Table),
            ts_setup:create_bucket_type(Cluster, DDL, Table),
            ts_setup:activate_bucket_type(Cluster, Table)
        end, test_tables()),
    %% failure on this line indicates the test is out-of-sync w/ current
    verify_dets_entries_current(),
    rt:stop(Node),
    simulate_old_dets_entries(),
    %% on start, Riak TS should recompile DDLs as needed
    rt:start(Node),
    rt:wait_for_service(Node, riak_kv),
    %% failure on this line indicates is either:
    %% 1. that the test is out-of-sync w/ simulating previous
    %% 2. that the feature-under-test is failing
    verify_dets_entries_current(),
    pass.

open_dets(_Table = ?CURRENT_TABLE) ->
    DetsPath = rtdev:riak_data(1),
    riak_kv_compile_tab:new(DetsPath);
open_dets(Table) ->
    FileDir = rtdev:riak_data(1),
    DetsPath = filename:join(FileDir, [Table, ".dets"]),
    {ok, Table} = dets:open_file(Table, [{type, set}, {repair, force}, {file, DetsPath}]).

simulate_old_dets_entries() ->
    open_dets(?CURRENT_TABLE),
    open_dets(?PREVIOUS_TABLE),
    Table1DDL = sql_to_ddl(create_table_sql("Table1")),
    Table2DDL = sql_to_ddl(create_table_sql("Table2")),
    Table3DDL = sql_to_ddl(create_table_sql("Table3")),

    %% Here we want to test 3 degenerate cases:
    %% 1) An old DETS entry in compiled state
    %% 2) An old DETS entry in compiling state
    %% 3) A current, valid DETS entry
    ok = riak_kv_compile_tab:insert_previous(<<"Table1">>, Table1DDL, compiled),
    ok = riak_kv_compile_tab:insert_previous(<<"Table2">>, Table2DDL, compiling),
    ok = riak_kv_compile_tab:insert(<<"Table3">>, Table3DDL),
    dets:close(?PREVIOUS_TABLE),
    dets:close(?CURRENT_TABLE).

verify_dets_entries_current() ->
    CurrentV = v2,
    open_dets(?CURRENT_TABLE),
    open_dets(?PREVIOUS_TABLE),

    lists:foreach(fun(T) ->
        Versions = riak_kv_compile_tab:get_compiled_ddl_versions(T),
        ContainsCurrentV = lists:any(fun(V) -> V =:= CurrentV end, Versions),
        ?assertEqual(
           {true, T, Versions},
           {ContainsCurrentV, T, Versions})
        end, test_tables()),
    dets:close(?PREVIOUS_TABLE),
    dets:close(?CURRENT_TABLE).

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
