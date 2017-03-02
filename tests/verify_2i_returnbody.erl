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
-module(verify_2i_returnbody).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-import(secondary_index_tests, [put_an_object/2, put_an_object/4, int_to_key/1,
                                query_to_url/3,
                                pb_query/3, stream_pb/3, http_query/3]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).
-define(Q_OPTS, [{return_body, true}]).
-define(NUM_KEYS, 1000).

confirm() ->
    inets:start(),

    rt:set_backend(eleveldb),
    Nodes = rt:build_cluster(3),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    RiakHttp = rt:http_url(hd(Nodes)),
    PBPid = rt:pbc(hd(tl(Nodes))),

    CustomQuery = {<<"field1_bin">>, ?FOO},
    %% KeyQuery = {<<"$key">>, int_to_key(0), int_to_key(999)},
    %% BucketQuery = {<<"$bucket">>, ?BUCKET},

    [put_an_object(PBPid, N) || N <- lists:seq(0, ?NUM_KEYS)],

    %% Verify that return_body + custom index fails

    %% PB
    ?assertMatch({error, _}, pb_query(PBPid, CustomQuery, ?Q_OPTS)),

    %% HTTP
    Url = query_to_url(RiakHttp, CustomQuery, ?Q_OPTS),
    {ok, {HTTPStatus, _, Body}} = httpc:request(Url),
    ?assertMatch({"HTTP/1.1",400,"Bad Request"},
                 HTTPStatus),
    %% Make certain the body of the response is complaining about the
    %% return_body parameter
    ?assert(string:str(Body, "return_body") > 0),

    %% Verify that all bodies are returned correctly

    ExpectedTuples = lists:sort([{int_to_key(N), list_to_binary(io_lib:format("data~p", [N]))}
                                 || N <- lists:seq(0, ?NUM_KEYS)]),
    assertEqual(RiakHttp, PBPid, ExpectedTuples, { <<"$bucket">>, ?BUCKET}, ?Q_OPTS),

    pass.

%% Check the PB result against our expectations
%% and the non-streamed HTTP
assertEqual(Http, PB, Expected, Query, Opts) ->
    {ok, PBStreamData0} = stream_pb(PB, Query, Opts),
    PBStreamData1 = proplists:get_value(results, PBStreamData0, []),
    PBStreamData = lists:map(
                     fun(O) ->
                             {riakc_obj:key(O), riakc_obj:get_value(O)}
                     end,
                     PBStreamData1),
    {ok, ?INDEX_BODY_RESULTS{objects=PBData0}} = pb_query(PB, Query, Opts),
    PBData = lists:map(
               fun(O) ->
                       {riakc_obj:key(O), riakc_obj:get_value(O)}
               end,
               PBData0),

    HTTPRes = http_query(Http, Query, Opts),

    HTTPResults0 = proplists:get_value(<<"results">>, HTTPRes, []),
    HTTPResults = lists:map(fun decode_http/1, HTTPResults0),
    ?assertEqual(Expected, lists:sort(PBData)),
    ?assertEqual(Expected, lists:sort(PBStreamData)),
    ?assertEqual(Expected, lists:sort(HTTPResults)).

decode_http({struct, [{Key, Val}]}) ->
    {Key, base64:decode(Val)}.
