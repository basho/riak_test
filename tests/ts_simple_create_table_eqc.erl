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
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_NVAL, 3).

-export([
         confirm/0
        ]).

confirm() ->
    ClusterConn = ts_util:cluster_and_connect(multiple),
    ?assert(eqc:quickcheck(eqc:numtests(500, ?MODULE:prop_ts(ClusterConn)))),
    pass.

prop_ts(ClusterConn) ->
    ?FORALL({Table, DDL},
            ts_sql_eqc_util:gen_valid_create_table(),
            run_query(ClusterConn, Table, DDL)).

run_query(ClusterConn, Table, DDL) ->
    lager:info("Creating table ~p~n- from ~p~n", [Table, DDL]),
    {ok, _} = ts_util:create_and_activate_bucket_type(ClusterConn, DDL, Table, ?DEFAULT_NVAL),
    true.

