%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(ts_simple_query_buffers_oom).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("ts_qbuf_util.hrl").

-define(RIDICULOUSLY_SMALL_MAX_QUERY_DATA_SIZE, 100).

confirm() ->
    [Node] = Cluster = ts_util:build_cluster(single),
    Data = ts_qbuf_util:make_data(),
    Config = [{cluster, Cluster}, {data, Data}],

    C = rt:pbc(Node),
    ok = ts_qbuf_util:create_table(C, ?TABLE),
    ok = ts_qbuf_util:insert_data(C, ?TABLE,  Data),

    ok = rpc:call(Node, riak_kv_qry_buffers, set_max_query_data_size, [?RIDICULOUSLY_SMALL_MAX_QUERY_DATA_SIZE]),
    Query1 = ts_qbuf_util:full_query(?TABLE, [{order_by, [{"d", undefined, undefined}]}]),
    ok = ts_qbuf_util:ack_query_error(Config, Query1, 1022),   %% error codes defined in riak_kv_ts_svc.erl

    Query2 = ts_qbuf_util:full_query(?TABLE, [{order_by, [{"d", undefined, undefined}]},
                                              {limit, 99999}]),
    ok = ts_qbuf_util:ack_query_error(Config, Query2, 1022),

    pass.
