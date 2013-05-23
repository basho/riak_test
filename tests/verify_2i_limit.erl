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
-module(verify_2i_limit).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-import(secondary_index_tests, [put_an_object/2, int_to_key/1,
                                stream_pb/3, http_query/3, pb_query/3]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).
-define(MAX_RESULTS, 50).

confirm() ->
    inets:start(),

    Nodes = rt:build_cluster(3),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    RiakHttp = rt:http_url(hd(Nodes)),
    PBPid = rt:pbc(hd(Nodes)),

    [put_an_object(PBPid, N) || N <- lists:seq(0, 100)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 100)]),
    {FirstHalf, Rest} = lists:split(?MAX_RESULTS, ExpectedKeys),
    Q = {<<"$key">>, int_to_key(0), int_to_key(999)},

    %% PB
    {ok, PBRes} = stream_pb(PBPid, Q, [{max_results, ?MAX_RESULTS}]),
    ?assertEqual(FirstHalf, proplists:get_value(keys, PBRes, [])),
    PBContinuation = proplists:get_value(continuation, PBRes),

    {ok, PBKeys2} = stream_pb(PBPid, Q, [{continuation, PBContinuation}]),
    ?assertEqual(Rest, proplists:get_value(keys, PBKeys2, [])),

    %% HTTP
    HttpRes = http_query(RiakHttp, Q, [{max_results, ?MAX_RESULTS}]),
    ?assertEqual(FirstHalf, proplists:get_value(<<"keys">>, HttpRes, [])),
    HttpContinuation = proplists:get_value(<<"continuation">>, HttpRes),
    ?assertEqual(PBContinuation, HttpContinuation),

    HttpRes2 = http_query(RiakHttp, Q, [{continuation, HttpContinuation}]),
    ?assertEqual(Rest, proplists:get_value(<<"keys">>, HttpRes2, [])),

    %% Multiple indexes for single key
    O1 = riakc_obj:new(?BUCKET, <<"bob">>, <<"1">>),
    Md = riakc_obj:get_metadata(O1),
    Md2 = riakc_obj:set_secondary_index(Md, {{integer_index, "i1"}, [300, 301, 302]}),
    O2 = riakc_obj:update_metadata(O1, Md2),
    riakc_pb_socket:put(PBPid, O2),

    MQ = {"i1_int", 300, 302},
    {ok, Res} = pb_query(PBPid, MQ, [{max_results, 2}, return_terms]),

    ?assertEqual([{<<"300">>, <<"bob">>},
                  {<<"301">>, <<"bob">>}], proplists:get_value(results, Res)),

    {ok, Res2} = pb_query(PBPid, MQ, [{max_results, 2}, return_terms,
                                      {continuation, proplists:get_value(continuation, Res)}]),

    ?assertEqual({results,[{<<"302">>,<<"bob">>}]}, Res2),

    riakc_pb_socket:stop(PBPid),
    pass.
