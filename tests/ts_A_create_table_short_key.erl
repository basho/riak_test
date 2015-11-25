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

-module(ts_A_create_table_short_key).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
     confirm/0
    ]).

confirm() ->
    ClusterType = single,
    DDL = ts_util:get_ddl(shortkey_fail),
    Expected = {ok,"Error validating table definition for bucket type GeoCheckin:\nPrimary key must consist of exactly 3 fields (has 2)\n"},
    Got = ts_util:create_bucket_type(ts_util:build_cluster(ClusterType), DDL),
    ?assertEqual(Expected, Got),
    pass.
