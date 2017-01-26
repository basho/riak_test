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

-module(ts_simple_show_create_table).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    Table = "ShowCreateTable",
    Cluster = initialize(Table),

    ShowQry = "SHOW CREATE TABLE " ++ Table,
    {ok, {_, [{AfterCreate}]}} = ts_ops:query(Cluster, ShowQry),

    AlterQry = "ALTER TABLE " ++ Table ++
        " WITH (n_val = 4)",
    {ok, _} = ts_ops:query(Cluster, AlterQry),

    {ok, {_, [{AfterAlter}]}} = ts_ops:query(Cluster, ShowQry),

    ExpectedBefore = "CREATE TABLE " ++ Table ++ " ("
    "somechars VARCHAR NOT NULL,\n"
    "somebool BOOLEAN NOT NULL,\n"
    "sometime TIMESTAMP NOT NULL,\n"
    "somefloat DOUBLE,\n"
    "PRIMARY KEY ((somechars, somebool, QUANTUM(sometime, 1, 'h')),\n"
    "somechars, somebool, sometime))\n"
    "WITH (active = true,\n"
    "allow_mult = false,\n"
    "dvv_enabled = false,\n"
    "dw = one,\n"
    "last_write_wins = true,\n"
    "n_val = 2,\n"
    "notfound_ok = true,\n"
    "postcommit = '',\n"
    "pr = 0,\n"
    "pw = 0,\n"
    "r = one,\n"
    "rw = one,\n"
    "w = quorum)",

    ExpectedAfter = "CREATE TABLE " ++ Table ++ " ("
    "somechars VARCHAR NOT NULL,\n"
    "somebool BOOLEAN NOT NULL,\n"
    "sometime TIMESTAMP NOT NULL,\n"
    "somefloat DOUBLE,\n"
    "PRIMARY KEY ((somechars, somebool, QUANTUM(sometime, 1, 'h')),\n"
    "somechars, somebool, sometime))\n"
    "WITH (active = true,\n"
    "allow_mult = false,\n"
    "dvv_enabled = false,\n"
    "dw = one,\n"
    "last_write_wins = true,\n"
    "n_val = 4,\n"
    "notfound_ok = true,\n"
    "postcommit = '',\n"
    "pr = 0,\n"
    "pw = 0,\n"
    "r = one,\n"
    "rw = one,\n"
    "w = quorum)",
    ?assertEqual(ExpectedBefore, AfterCreate),
    ?assertEqual(ExpectedAfter, AfterAlter),
    pass.

initialize(Table) ->
    DDL = "CREATE TABLE " ++ Table ++ " ("
    " somechars   VARCHAR   NOT NULL,"
    " somebool    BOOLEAN   NOT NULL,"
    " sometime    TIMESTAMP NOT NULL,"
    " somefloat   DOUBLE,"
    " PRIMARY KEY ((somechars, somebool, QUANTUM(sometime, 1, 'h')), "
    " somechars, somebool, sometime)) "
    " WITH (n_val=2)",
    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    Cluster.
