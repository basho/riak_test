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

-module(ts_simple_put_invalid_data).

%%
%% this test tries to write well structured data that doesn't
%% meet the criteria defined in the DDL into a bucket
%%

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    DDL = ts_util:get_ddl(),
    ValidObj = ts_util:get_valid_obj(),
    InvalidObj = ts_util:get_invalid_obj(),
    ShortObj = ts_util:get_short_obj(),
    LongObj = ts_util:get_long_obj(),
    Bucket = ts_util:get_default_bucket(),
    {_Cluster, Conn} = ClusterConn = ts_util:cluster_and_connect(single),
    Expected1 = {error, {1003, <<"Invalid data found at row index(es) 1">>}},
    Expected2 = {error, {1003, <<"Invalid data found at row index(es) 2">>}},
    Got = ts_util:ts_put(ClusterConn, normal, DDL, [InvalidObj]),
    ?assertEqual(Expected1, Got),

    Got2 = riakc_ts:put(Conn, Bucket, [ShortObj]),
    ?assertEqual(Expected1, Got2),

    Got3 = riakc_ts:put(Conn, Bucket, [LongObj]),
    ?assertEqual(Expected1, Got3),

    Got4 = riakc_ts:put(Conn, Bucket, [ValidObj, InvalidObj]),
    ?assertEqual(Expected2, Got4),

    Got5 = riakc_ts:put(Conn, Bucket, [ValidObj, ShortObj]),
    ?assertEqual(Expected2, Got5),

    Got6 = riakc_ts:put(Conn, Bucket, [ValidObj, LongObj]),
    ?assertEqual(Expected2, Got6),
    pass.
