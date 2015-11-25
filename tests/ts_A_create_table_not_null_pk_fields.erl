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

-module(ts_A_create_table_not_null_pk_fields).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    ClusterType = single,
    DDL = ts_util:get_ddl(not_null_primary_key_field_fail),
    Expected = {ok, "Error creating bucket type GeoCheckin:\nAll fields in primary key must be not null\n\n"},
    Got = ts_util:create_bucket_type(ts_util:build_cluster(ClusterType), DDL),
    ?assertEqual(Expected, Got),
    pass.
