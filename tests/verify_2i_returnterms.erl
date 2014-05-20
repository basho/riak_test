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
-module(verify_2i_returnterms).
-behavior(riak_test).

-export([confirm/0]).
-export([confirm/2]).

-import(secondary_index_tests, [put_an_object/3, put_an_object/5, int_to_key/1,
                               stream_pb/4, http_query/4]).

-include_lib("eunit/include/eunit.hrl").

-define(FOO, <<"foo">>).
-define(Q_OPTS, [{return_terms, true}]).

confirm() ->
    inets:start(),
    Nodes = rt:build_cluster(3),
    confirm(<<"2ireturnterms">>, Nodes).

confirm(Bucket, Nodes) ->
    RiakHttp = rt:http_url(hd(Nodes)),
    PBPid = rt:pbc(hd(Nodes)),

    [put_an_object(PBPid, Bucket, N) || N <- lists:seq(0, 100)],
    [put_an_object(PBPid, Bucket, int_to_key(N), N, ?FOO) || N <- lists:seq(101, 200)],

    %% Bucket, key, and index_eq queries should ignore `return_terms'
    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 200)]),
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedKeys, {<<"$key">>, int_to_key(0), int_to_key(999)}, ?Q_OPTS, keys),
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedKeys, { <<"$bucket">>, Bucket}, ?Q_OPTS, keys),

    ExpectedFooKeys = lists:sort([int_to_key(N) || N <- lists:seq(101, 200)]),
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedFooKeys, {<<"field1_bin">>, ?FOO}, ?Q_OPTS, keys),

    ExpectedRangeResults = lists:sort([{list_to_binary(integer_to_list(N)), int_to_key(N)} || N <- lists:seq(1, 100)]),
    assertEqual(RiakHttp, PBPid, Bucket, ExpectedRangeResults, {<<"field2_int">>, "1", "100"}, ?Q_OPTS, results),

    riakc_pb_socket:stop(PBPid),
    pass.

%% Check the PB result against our expectations
%% and the non-streamed HTTP
assertEqual(Http, PB, Bucket, Expected, Query, Opts, ResultKey) ->
    {ok, PBRes} = stream_pb(PB, Bucket, Query, Opts),
    PBKeys = proplists:get_value(ResultKey, PBRes, []),
    HTTPRes = http_query(Http, Bucket, Query, Opts),
    HTTPResults0 = proplists:get_value(atom_to_binary(ResultKey, latin1), HTTPRes, []),
    HTTPResults = decode_http_results(ResultKey, HTTPResults0),
    ?assertEqual(Expected, lists:sort(PBKeys)),
    ?assertEqual(Expected, lists:sort(HTTPResults)).

decode_http_results(keys, Keys) ->
    Keys;
decode_http_results(results, Results) ->
    decode_http_results(Results, []);

decode_http_results([], Acc) ->
    lists:reverse(Acc);
decode_http_results([{struct, [Res]} | Rest ], Acc) ->
    decode_http_results(Rest, [Res | Acc]).

