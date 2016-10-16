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

-module(ts_simple_query_buffers_ldb_errors).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("ts_qbuf_util.hrl").

confirm() ->
    [Node] = Cluster = ts_util:build_cluster(single),
    Data = ts_qbuf_util:make_data(),
    Config = [{cluster, Cluster}, {data, Data}],

    C = rt:pbc(Node),
    ok = ts_qbuf_util:create_table(C, ?TABLE),
    ok = ts_qbuf_util:insert_data(C, ?TABLE,  Data),

    %% issue a query to create the dev1/data/.../query_buffers dir
    Query = ts_qbuf_util:full_query(?TABLE, [{order_by, [{"d", undefined, undefined}]}]),

    %% chmod the query buffer dir out of existence
    QBufDir = filename:join([rtdev:node_path(Node), "data/leveldb/query_buffers"]),
    Cmd = fmt("chmod -w '~s'", [QBufDir]),
    CmdOut = "" = os:cmd(Cmd),
    io:format("~s: '~s'", [Cmd, CmdOut]),

    Res = ts_qbuf_util:ack_query_error(Config, Query, 1023),

    %% reverse the w perm so that r_t harness can proceed with clean-up
    Cmd2 = fmt("chmod +w '~s'", [QBufDir]),
    "" = os:cmd(Cmd2),

    ?assertEqual(Res, ok),
    pass.

fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
