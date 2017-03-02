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

-module(ts_simple_select_incompatible_type_float_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),
    Data = ts_data:get_valid_select_data(),
    Qry =
        "SELECT * FROM GeoCheckin "
        "WHERE time > 1 AND time < 10 "
        "AND myfamily = 'family1' "
        "AND myseries = 1.0", % error, should be a varchar
    Expected =
        {error,
            {1001,
                <<".*incompatible_type: field myseries with type varchar cannot be compared to type float in where clause.">>}},

    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    ts_ops:put(Cluster, Table, Data),
    Got = ts_ops:query(Cluster, Qry),
    ts_data:assert_error_regex("Incompatible types", Expected, Got).
