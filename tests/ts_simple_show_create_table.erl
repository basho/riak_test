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
    DDL = "CREATE TABLE " ++ Table ++ " ("
    " somechars   VARCHAR   NOT NULL,"
    " somebool    BOOLEAN   NOT NULL,"
    " sometime    TIMESTAMP NOT NULL,"
    " somefloat   DOUBLE,"
    " PRIMARY KEY ((somechars, somebool, QUANTUM(sometime, 1, 'h')), "
    " somechars, somebool, sometime)) "
    " WITH (n_val=2)",
    Qry = "SHOW CREATE TABLE " ++ Table,
    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    {ok, {_, [{Got}]}} = ts_ops:query(Cluster, Qry),
    Expected = "CREATE TABLE " ++ Table ++ " ("
    "somechars VARCHAR NOT NULL,\n"
    "somebool BOOLEAN NOT NULL,\n"
    "sometime TIMESTAMP NOT NULL,\n"
    "somefloat DOUBLE,\n"
    "PRIMARY KEY ((somechars, somebool, QUANTUM(sometime, 1, 'h')),\n"
    "somechars, somebool, sometime))\n"
    "WITH (active = true,\n"
    "allow_mult = true,\n"
    "dvv_enabled = true,\n"
    "dw = quorum,\n"
    "last_write_wins = false,\n"
    "n_val = 2,\n"
    "notfound_ok = true,\n"
    "postcommit = '',\n"
    "pr = 0,\n"
    "pw = 0,\n"
    "r = quorum,\n"
    "rw = quorum,\n"
    "w = quorum)",
    ?assertEqual(Expected, Got),
    pass.
