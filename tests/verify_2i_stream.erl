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
-export([confirm/1]).
-import(secondary_index_tests, [put_an_object/3, put_an_object/5, int_to_key/1,
                               stream_pb/3, http_query/3, http_stream/4]).
-include("include/rt.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(FOO, <<"foo">>).

confirm() ->
    inets:start(),
    Nodes = rt:build_cluster(3),
    confirm(#rt_test_context{buckets=[<<"2i_stream">>], nodes=Nodes}).

confirm(#rt_test_context{buckets=[Bucket|_], nodes=Nodes}) ->
    RiakHttp = rt:http_url(hd(Nodes)),
    PBPid = rt:pbc(hd(Nodes)),

    [put_an_object(PBPid, Bucket, N) || N <- lists:seq(0, 100)],
    [put_an_object(PBPid, Bucket, int_to_key(N), N, ?FOO) || N <- lists:seq(101, 200)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 200)]),
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedKeys, {<<"$key">>, int_to_key(0), int_to_key(999)}),
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedKeys, { <<"$bucket">>, Bucket}),

    ExpectedFooKeys = lists:sort([int_to_key(N) || N <- lists:seq(101, 200)]),
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedFooKeys, {<<"field1_bin">>, ?FOO}),

    %% Note: not sorted by key, but by value (the int index)
    ExpectedRangeKeys = [int_to_key(N) || N <- lists:seq(1, 100)],
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedRangeKeys, {<<"field2_int">>, "1", "100"}),

    riakc_pb_socket:stop(PBPid),
    pass.

%% Check the PB result against our expectations
%% and the non-streamed HTTP
assertEqual(Http, PB, Bucket, Expected0, Query) ->
    {ok, PBRes} = stream_pb(PB, Bucket, Query),
    PBKeys = proplists:get_value(keys, PBRes, []),
    HTTPRes = http_query(Http, Bucket, Query),
    StreamHTTPRes = http_stream(Http, Bucket, Query, []),
    HTTPKeys = proplists:get_value(<<"keys">>, HTTPRes, []),
    StreamHttpKeys = proplists:get_value(<<"keys">>, StreamHTTPRes, []),
    Expected = lists:sort(Expected0),
    ?assertEqual(Expected, lists:sort(PBKeys)),
    ?assertEqual(Expected, lists:sort(HTTPKeys)),
    ?assertEqual(Expected, lists:sort(StreamHttpKeys)).

