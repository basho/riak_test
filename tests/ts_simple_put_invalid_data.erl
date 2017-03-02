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

-module(ts_simple_put_invalid_data).

%%
%% this test tries to write well structured data that doesn't
%% meet the criteria defined in the DDL into a bucket
%%

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),

    Cluster = ts_setup:start_cluster(1),
    Conn = ts_setup:conn(Cluster),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    
    ValidObj = ts_data:get_valid_obj(),
    InvalidObj = ts_data:get_invalid_obj(),
    ShortObj = ts_data:get_short_obj(),
    LongObj = ts_data:get_long_obj(),

    Expected1 = {error, {1003, <<"Invalid data found at row index(es) 1">>}},
    Expected2 = {error, {1003, <<"Invalid data found at row index(es) 2">>}},
    Got = riakc_ts:put(Conn, Table, [InvalidObj]),
    ?assertEqual(Expected1, Got),

    Got2 = riakc_ts:put(Conn, Table, [ShortObj]),
    ?assertEqual(Expected1, Got2),

    Got3 = riakc_ts:put(Conn, Table, [LongObj]),
    ?assertEqual(Expected1, Got3),

    Got4 = riakc_ts:put(Conn, Table, [ValidObj, InvalidObj]),
    ?assertEqual(Expected2, Got4),

    Got5 = riakc_ts:put(Conn, Table, [ValidObj, ShortObj]),
    ?assertEqual(Expected2, Got5),

    Got6 = riakc_ts:put(Conn, Table, [ValidObj, LongObj]),
    ?assertEqual(Expected2, Got6),
    pass.
