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

confirm() ->
    [Node | _Rest] = Cluster = ts_setup:start_cluster(1),
    lists:foreach(
        fun(Table) ->
            DDL = create_table_sql(Table),
            ts_setup:create_bucket_type(Cluster, DDL, Table),
            ts_setup:activate_bucket_type(Cluster, Table)
        end, test_tables()),
    %% failure on this line indicates the test is out-of-sync w/ current
    verify_compile_tab_current(Node),
    simulate_compile_tab_previous(Node),
    rt:stop(Node),
    %% on start, Riak TS should recompile DDLs as needed
    rt:start(Node),
    rt:wait_for_service(Node, riak_kv),
    %% failure on this line indicates is either:
    %% 1. that the test is out-of-sync w/ simulating previous
    %% 2. that the feature-under-test is failing
    verify_compile_tab_current(Node),
    pass.

%% compile_tab_open(Node) ->
%%     DetsPath = rtdev:riak_data(1),
%%     riak_kv_compile_tab:new(DetsPath).

insert_previous(Node, Table, DDL, CompilerState) ->
    ok = rpc:call(Node, riak_kv_compile_tab, insert_previous,
                  [Table, DDL, CompilerState]).

insert(Node, Table, DDL) ->
    ok = rpc:call(Node, riak_kv_compile_tab, insert,
                  [Table, DDL]).

simulate_compile_tab_previous(Node) ->
    Table1DDL = sql_to_ddl(create_table_sql("Table1")),
    Table2DDL = sql_to_ddl(create_table_sql("Table2")),
    Table3DDL = sql_to_ddl(create_table_sql("Table3")),

    %% Here we want to test 3 degenerate cases:
    %% 1) An old DETS entry in compiled state
    %% 2) An old DETS entry in compiling state
    %% 3) A current, valid DETS entry
    insert_previous(Node, <<"Table1">>, Table1DDL, compiled),
    insert_previous(Node, <<"Table2">>, Table2DDL, compiling),
    insert(Node, <<"Table3">>, Table3DDL).

assert_compiled_ddl_versions_current(Node, Table) ->
    ?assertEqual(ok,
                 rpc:call(Node, riak_kv_compile_tab, assert_compiled_ddl_versions_current,
                          [Table])).

verify_compile_tab_current(Node) ->
    lists:foreach(fun(T) ->
                          assert_compiled_ddl_versions_current(Node, T)
                  end, test_tables()).

test_tables() ->
    [<<"Table1">>,<<"Table2">>,<<"Table3">>].

create_table_sql(TableName) ->
    % presence of a field that isn't allowed for downgrade, i.e. BLOB for
    % 1.4 -> 1.5, should not be present. Such assertions should be tested
    % in riak_ql and/or riak_kv, not in riak_test.

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
