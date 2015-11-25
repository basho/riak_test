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

-module(ts_A_create_table_already_created).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

%%
%% should error if you try and create a table twice
%%

confirm() ->
    ClusterType = single,
    DDL = ts_util:get_ddl(docs),
    ClusterConn = ts_util:cluster_and_connect(ClusterType),
    Expected1 = {ok, "GeoCheckin has been activated\n\nWARNING: Nodes in this cluster can no longer be\ndowngraded to a version of Riak prior to 2.0\n"},
    Got1 = ts_util:create_and_activate_bucket_type(ClusterConn, DDL),
    ?assertEqual(Expected1, Got1),
    Expected2 = {ok, "Error creating bucket type GeoCheckin:\nalready_active\n"},
    Got2 = ts_util:create_bucket_type(ClusterConn, DDL),
    ?assertEqual(Expected2, Got2),
    pass.
