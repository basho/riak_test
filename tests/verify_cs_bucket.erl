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

%% @ doc tests the new CS Bucket fold message

-module(verify_cs_bucket).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-import(secondary_index_tests, [put_an_object/2, int_to_key/1]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).

confirm() ->
    Nodes = rt:build_cluster(3),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    PBPid = rt:pbc(hd(Nodes)),

    [put_an_object(PBPid, N) || N <- lists:seq(0, 200)],

    ExpectedKeys = lists:sort([int_to_key(N) || N <- lists:seq(0, 200)]),

    undefined = assertEqual(PBPid, ExpectedKeys, ?BUCKET,
                            [{start_key, int_to_key(0)}]),
    undefined =  assertEqual(PBPid, tl(ExpectedKeys), ?BUCKET,
                            [{start_key, int_to_key(0)}, {start_incl, false}]),
    undefined = assertEqual(PBPid,
                            [int_to_key(104)],
                            ?BUCKET,
                            [{start_key, int_to_key(103)},
                                {end_key, int_to_key(105)},
                                {start_incl, false},
                                {end_incl, false}]),
    undefined = assertEqual(PBPid,
                                [int_to_key(104), int_to_key(105)],
                                ?BUCKET,
                                [{start_key, int_to_key(103)},
                                    {end_key, int_to_key(105)},
                                    {start_incl, false},
                                    {end_incl, true}]),
    undefined = assertEqual(PBPid,
                                [int_to_key(103), int_to_key(104), int_to_key(105)],
                                ?BUCKET,
                                [{start_key, int_to_key(103)},
                                    {end_key, int_to_key(105)},
                                    {start_incl, true},
                                    {end_incl, true}]),

    %% Limit / continuations

    Continuation1 = assertEqual(PBPid, lists:sublist(ExpectedKeys, 20), ?BUCKET, [{start_key, int_to_key(0)}, {max_results, 20}]),
    Continuation2 = assertEqual(PBPid, lists:sublist(ExpectedKeys, 21, 20), ?BUCKET,
                                [{start_key, int_to_key(0)}, {max_results, 20}, {continuation, Continuation1}]),
    undefined = assertEqual(PBPid, lists:sublist(ExpectedKeys, 41, 200), ?BUCKET, [{continuation, Continuation2}, {max_results, 200}]),

    riakc_pb_socket:stop(PBPid),
    pass.

%% Check the PB result against our expectations
%% and the non-streamed HTTP
assertEqual(PB, Expected, Bucket, Opts) ->
    {ok, PBRes} = stream_pb(PB, Bucket, Opts),
    PBObjects = proplists:get_value(objects, PBRes, []),
    Keys = [riakc_obj:key(Obj) || Obj <- PBObjects],
    ?assertEqual(Expected, Keys),
    proplists:get_value(continuation, PBRes).


stream_pb(Pid, Bucket, Opts) ->
    riakc_pb_socket:cs_bucket_fold(Pid, Bucket, Opts),
    stream_loop().

stream_loop() ->
    stream_loop(orddict:new()).

stream_loop(Acc) ->
    receive
        {_Ref, {done, undefined}} ->
            {ok, orddict:to_list(Acc)};
        {_Ref, {done, Continuation}} ->
            {ok, orddict:store(continuation, Continuation, Acc)};
        {_Ref, {ok, Objects}} ->
            Acc2 = orddict:update(objects, fun(Existing) -> Existing++Objects end, Objects, Acc),
            stream_loop(Acc2)
    end.
