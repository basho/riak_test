%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(verify_2i_timeout).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-import(secondary_index_tests, [put_an_object/2, put_an_object/4, int_to_key/1,
                               stream_pb/3, url/2, http_query/3, http_stream/3]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).

confirm() ->
    inets:start(),
    Config = [{riak_kv, [{secondary_index_timeout, 1}]}], %% ludicrously short, should fail always
    Nodes = rt_cluster:build_cluster([{current, Config}, {current, Config}, {current, Config}]),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    PBPid = rt:pbc(hd(Nodes)),
    Http = rt:http_url(hd(Nodes)),

    [put_an_object(PBPid, N) || N <- lists:seq(0, 100)],
    [put_an_object(PBPid, int_to_key(N), N, ?FOO) || N <- lists:seq(101, 200)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 200)]),
    Query = {<<"$bucket">>, ?BUCKET},
    %% Verifies that the app.config param was used
    ?assertEqual({error, timeout}, stream_pb(PBPid, Query, [])),

    %% Override app.config
    {ok, Res} =  stream_pb(PBPid, Query, [{timeout, 5000}]),
    ?assertEqual(ExpectedKeys, lists:sort(proplists:get_value(keys, Res, []))),

    {ok, {{_, ErrCode, _}, _, Body}} = httpc:request(url("~s/buckets/~s/index/~s/~s~s",
                                                     [Http, ?BUCKET, <<"$bucket">>, ?BUCKET, []])),

    ?assertEqual(true, ErrCode >= 500),
    ?assertMatch({match, _}, re:run(Body, "request timed out|{error,timeout}")), %% shows the app.config timeout

    HttpRes = http_query(Http, Query, [{timeout, 5000}]),
    ?assertEqual(ExpectedKeys, lists:sort(proplists:get_value(<<"keys">>, HttpRes, []))),

    stream_http(Http, Query, ExpectedKeys),

    riakc_pb_socket:stop(PBPid),
    pass.

stream_http(Http, Query, ExpectedKeys) ->
     Res = http_stream(Http, Query, []),
     ?assert(lists:member({<<"error">>,<<"timeout">>}, Res)),
     Res2 = http_stream(Http, Query, [{timeout, 5000}]),
     ?assertEqual(ExpectedKeys, lists:sort(proplists:get_value(<<"keys">>, Res2, []))).

