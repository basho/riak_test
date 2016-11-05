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
%% @doc A module to test riak_ts try to create a bucket type when one
%%      node is down.

-module(ts_degraded_activate_table_fail_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    DDL = ts_data:get_ddl(),
    Expected = {ok, "GeoCheckin has been created but cannot be activated yet\n"},

    [_Node|Rest]= Cluster = ts_setup:start_cluster(3),
    ok = rt:stop(hd(tl(Rest))),
    Table = ts_data:get_default_bucket(),
    {ok,_} = ts_setup:create_bucket_type(Cluster, DDL, Table),
    Got = ts_setup:activate_bucket_type(Cluster, Table),
    ?assertEqual(Expected, Got),
    pass.
