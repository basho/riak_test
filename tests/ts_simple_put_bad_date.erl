%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2016 Basho Technologies, Inc.
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

-module(ts_simple_put_bad_date).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),
    Obj =
        [{ts_data:get_varchar(),
            ts_data:get_varchar(),
            <<"abc">>,
            ts_data:get_varchar(),
            ts_data:get_float()}],
    Expected = {error, {1003, <<"Invalid data found at row index(es) 1">>}},

    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    Got = ts_ops:put(Cluster, Table, Obj),
    ?assertEqual(Expected, Got),
    pass.
