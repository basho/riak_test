%% -*- Mode: Erlang -*-
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

-module(ts_simple_create_table_eqc).
-compile(export_all).

-behavior(riak_test).

%-ifdef(EQC).
-include_lib("riakc/include/riakc.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(DEFAULT_NVAL, 3).

-export([


         confirm/0
        ]).

confirm() ->
    ClusterConn = ts_util:cluster_and_connect(multiple),
    ?assert(eqc:quickcheck(eqc:numtests(100, ?MODULE:prop_ts(ClusterConn)))),
    pass.

prop_ts(ClusterConn) ->
    ?FORALL(DDL,
            ts_sql_eqc_util:gen_valid_create_table("1.3"),
            create_and_activate(ClusterConn, DDL)).

create_and_activate(ClusterConn, #ddl_v1{table = Table} = DDL) ->
    Postfix = ts_sql_eqc_util:make_timestamp(),
    Table2 = list_to_binary(binary_to_list(Table) ++ Postfix),
    SQL = riak_ql_to_string:ddl_rec_to_sql(DDL#ddl_v1{table = Table2}),
    lager:info("Creating table ~p~n", [SQL]),
    {ok, _} = ts_util:create_and_activate_bucket_type(ClusterConn, SQL, Table2, 
                                                      ?DEFAULT_NVAL),
    true.


