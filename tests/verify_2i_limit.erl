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
-include_lib("riakc/include/riakc.hrl").
-import(secondary_index_tests, [put_an_object/2, put_an_object/4, int_to_key/1,
                                stream_pb/3, http_query/3, pb_query/3]).
-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).
-define(MAX_RESULTS, 50).

confirm() ->
    inets:start(),

    Nodes = rt_cluster:build_cluster(3),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    RiakHttp = rt_http:httpc(hd(Nodes)),
    HttpUrl = rt:http_url(hd(Nodes)),
    PBPid = rt_pb:pbc(hd(Nodes)),

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
    HttpRes = rhc:get_index(RiakHttp, ?BUCKET, <<"$key">>,
                            {int_to_key(0), int_to_key(999)},
                           [{max_results, ?MAX_RESULTS}]),
    ?assertMatch({ok, ?INDEX_RESULTS{}}, HttpRes),
    {ok, ?INDEX_RESULTS{keys=HttpResKeys,
                        continuation=HttpContinuation}} = HttpRes, 
    ?assertEqual(FirstHalf, HttpResKeys),
    ?assertEqual(PBContinuation, HttpContinuation),

    HttpRes2 = rhc:get_index(RiakHttp, ?BUCKET, <<"$key">>,
                             {int_to_key(0), int_to_key(999)},
                             [{continuation, HttpContinuation}]),
    ?assertMatch({ok, ?INDEX_RESULTS{}}, HttpRes2),
    {ok, ?INDEX_RESULTS{keys=HttpRes2Keys}} = HttpRes2,
    ?assertEqual(Rest, HttpRes2Keys),

    %% Multiple indexes for single key
    O1 = riakc_obj:new(?BUCKET, <<"bob">>, <<"1">>),
    Md = riakc_obj:get_metadata(O1),
    Md2 = riakc_obj:set_secondary_index(Md, {{integer_index, "i1"}, [300, 301, 302]}),
    O2 = riakc_obj:update_metadata(O1, Md2),
    riakc_pb_socket:put(PBPid, O2),

    MQ = {"i1_int", 300, 302},
    {ok, ?INDEX_RESULTS{terms=Terms, continuation=RTContinuation}} = pb_query(PBPid, MQ, [{max_results, 2}, return_terms]),

    ?assertEqual([{<<"300">>, <<"bob">>},
                  {<<"301">>, <<"bob">>}], Terms),

    {ok, ?INDEX_RESULTS{terms=Terms2}} = pb_query(PBPid, MQ, [{max_results, 2}, return_terms,
                                      {continuation, RTContinuation}]),

    ?assertEqual([{<<"302">>,<<"bob">>}], Terms2),

    %% gh611 - equals query pagination
    riakc_pb_socket:delete(PBPid, ?BUCKET, <<"bob">>),
    rt:wait_until(fun() -> rt_pb:pbc_really_deleted(PBPid, ?BUCKET, [<<"bob">>]) end),

    [put_an_object(PBPid, int_to_key(N), 1000, <<"myval">>) || N <- lists:seq(0, 100)],

    [ verify_eq_pag(PBPid, HttpUrl, EqualsQuery, FirstHalf, Rest) ||
        EqualsQuery <- [{"field1_bin", <<"myval">>},
                        {"field2_int", 1000},
                        {"$bucket", ?BUCKET}]],

    riakc_pb_socket:stop(PBPid),
    pass.

verify_eq_pag(PBPid, RiakHttp, EqualsQuery, FirstHalf, Rest) ->
    HTTPEqs = http_query(RiakHttp, EqualsQuery, [{max_results, ?MAX_RESULTS}]),
    ?assertEqual({EqualsQuery, FirstHalf},
                 {EqualsQuery, proplists:get_value(<<"keys">>, HTTPEqs, [])}),
    EqualsHttpContinuation = proplists:get_value(<<"continuation">>, HTTPEqs),

    HTTPEqs2 = http_query(RiakHttp, EqualsQuery,
                          [{continuation, EqualsHttpContinuation}]),
    ?assertEqual({EqualsQuery, Rest},
                 {EqualsQuery, proplists:get_value(<<"keys">>, HTTPEqs2, [])}),

    %% And PB

    {ok, EqPBRes} = stream_pb(PBPid, EqualsQuery,
                              [{max_results, ?MAX_RESULTS}]),
    ?assertEqual({EqualsQuery, FirstHalf},
                 {EqualsQuery, proplists:get_value(keys, EqPBRes, [])}),
    EqPBContinuation = proplists:get_value(continuation, EqPBRes),

    {ok, EqPBKeys2} = stream_pb(PBPid, EqualsQuery,
                                [{continuation, EqPBContinuation}]),
    ?assertEqual({EqualsQuery, Rest},
                 {EqualsQuery, proplists:get_value(keys, EqPBKeys2, [])}).
