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

-module(ts_simple_select_where_has_no_upper_bounds_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    DDL = ts_data:get_ddl(),
    Qry = "select * from GeoCheckin "
          "where time > 10 "
          "and myfamily = 'family1' "
          "and myseries ='seriesX' ",
    Expected =
        {error, {1001, <<"Where clause has no upper bound.">>}},
    Cluster = ts_setup:start_cluster(1),
    Table = ts_data:get_default_bucket(),
    {ok, _} = ts_setup:create_bucket_type(Cluster, DDL, Table),
    ok = ts_setup:activate_bucket_type(Cluster, Table),
    Got = ts_ops:query(Cluster, Qry),
    ts_data:assert_error_regex("No upper bound", Expected, Got).
