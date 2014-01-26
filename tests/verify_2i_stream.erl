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
-module(verify_2i_stream).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-import(secondary_index_tests, [put_an_object/2, put_an_object/4, int_to_key/1,
                               stream_pb/2, http_query/2, http_stream/3]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).

confirm() ->
    inets:start(),

    Nodes = rt:build_cluster(3),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    RiakHttp = rt:http_url(hd(Nodes)),
    PBPid = rt:pbc(hd(Nodes)),

    [put_an_object(PBPid, N) || N <- lists:seq(0, 100)],
    [put_an_object(PBPid, int_to_key(N), N, ?FOO) || N <- lists:seq(101, 200)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 200)]),
    assertEqual(RiakHttp, PBPid, ExpectedKeys, {<<"$key">>, int_to_key(0), int_to_key(999)}),
    assertEqual(RiakHttp, PBPid, ExpectedKeys, { <<"$bucket">>, ?BUCKET}),

    ExpectedFooKeys = lists:sort([int_to_key(N) || N <- lists:seq(101, 200)]),
    assertEqual(RiakHttp, PBPid, ExpectedFooKeys, {<<"field1_bin">>, ?FOO}),

    %% Note: not sorted by key, but by value (the int index)
    ExpectedRangeKeys = [int_to_key(N) || N <- lists:seq(1, 100)],
    assertEqual(RiakHttp, PBPid, ExpectedRangeKeys, {<<"field2_int">>, "1", "100"}),

    riakc_pb_socket:stop(PBPid),
    pass.

%% Check the PB result against our expectations
%% and the non-streamed HTTP
assertEqual(Http, PB, Expected0, Query) ->
    {ok, PBRes} = stream_pb(PB, Query),
    PBKeys = proplists:get_value(keys, PBRes, []),
    HTTPRes = http_query(Http, Query),
    StreamHTTPRes = http_stream(Http, Query, []),
    HTTPKeys = proplists:get_value(<<"keys">>, HTTPRes, []),
    StreamHttpKeys = proplists:get_value(<<"keys">>, StreamHTTPRes, []),
    Expected = lists:sort(Expected0),
    ?assertEqual(Expected, lists:sort(PBKeys)),
    ?assertEqual(Expected, lists:sort(HTTPKeys)),
    ?assertEqual(Expected, lists:sort(StreamHttpKeys)).

