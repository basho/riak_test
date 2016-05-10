%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%s
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Tests for the different combinations of keys supported by
%% Riak Time Series.
%%
%% -------------------------------------------------------------------
-module(ts_cluster_stream_query_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Cluster = ts_util:build_cluster(single),
    [{cluster, Cluster} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() ->
    rt:grep_test_functions(?MODULE).

client_pid(Ctx) ->
    [Node|_] = proplists:get_value(cluster, Ctx),
    rt:pbc(Node).

run_query(Ctx, Query) ->
    riakc_ts:query(client_pid(Ctx), Query).

%%%
%%%
%%%

stream_query_1_test(Ctx) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(client_pid(Ctx),
            "CREATE TABLE streamtable1 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))"
    )),
    ok = riakc_ts:put(client_pid(Ctx), <<"streamtable1">>,
        [{1,1,N} || N <- lists:seq(1, 100)]),
    Query =
        "SELECT * FROM streamtable1 WHERE a = 1 AND b = 1 AND c > 0 AND c < 11",
    {ok, ReqId} = riakc_ts:stream_query(
            client_pid(Ctx), Query, [], []),
    ts_util:ct_verify_rows(
        [{1,1,N} || N <- lists:seq(1, 10)],
        stream_query_receive(ReqId)
    ).

%%--------------------------------------------------------------------
%% UTILS
%%--------------------------------------------------------------------

stream_query_receive(ReqId) ->
    stream_query_receive2(ReqId, []).

stream_query_receive2(ReqId, Acc) ->
    receive
        {ReqId, {rows, Keys}} ->
            stream_query_receive2(ReqId, lists:append(Keys, Acc));
        {ReqId, {error, Reason}} ->
            {error, Reason};
        {ReqId, done} ->
            Acc;
        Else ->
            {error, {unknown_message, Else}}
    after 3000 ->
            {ok, Acc}
    end.
