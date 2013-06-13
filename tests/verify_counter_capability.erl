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
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2013, Basho Technologies
%%% @doc
%%% riak_test for counter cabability
%%% @end

-module(verify_counter_capability).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test-counters">>).
-define(KEY, <<"foo">>).

confirm() ->
    %% Create a mixed cluster of current and previous versions
    %% Create a PB client
    %% GET  / PUT on 1.4 and previous cluster
    %% Upgrade nodes
    %% Get put on all nodes
    Config = [],
    [Previous, Current]=Nodes = rt:build_cluster([{previous, Config}, {current, Config}]),
    ?assertEqual(ok, rt:wait_until_capability(Current, {riak_kv, crdt}, [])),
    verify_counter_converge:set_allow_mult_true(Nodes),

    {PrevPB, PrevHttp} = get_clients(Previous),
    {PB, Http} = get_clients(Current),

    ?assertMatch({error, {ok, "404", _, _}}, rhc:counter_incr(PrevHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({error, {ok, "404", _, _}}, rhc:counter_val(PrevHttp, ?BUCKET, ?KEY)),

    ?assertMatch({error, {ok, "503", _, _}}, rhc:counter_incr(Http, ?BUCKET, ?KEY, 1)),
    ?assertMatch({error, {ok, "503", _, _}}, rhc:counter_val(Http, ?BUCKET, ?KEY)),

    ?assertEqual({error,<<"Unknown message code.">>}, riakc_pb_socket:counter_incr(PrevPB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({error,<<"Unknown message code.">>}, riakc_pb_socket:counter_val(PrevPB, ?BUCKET, ?KEY)),
    ?assertEqual({error,<<"\"Counters are not supported\"">>}, riakc_pb_socket:counter_incr(PB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({error,<<"\"Counters are not supported\"">>}, riakc_pb_socket:counter_val(PB, ?BUCKET, ?KEY)),

    rt:upgrade(Previous, current),

    ?assertEqual(ok, rt:wait_until_capability(Current, {riak_kv, crdt}, [pncounter])),

    ?assertMatch(ok, rhc:counter_incr(PrevHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 1}, rhc:counter_val(PrevHttp, ?BUCKET, ?KEY)),

    ?assertMatch(ok, rhc:counter_incr(Http, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 2}, rhc:counter_val(Http, ?BUCKET, ?KEY)),

    ?assertEqual(ok, riakc_pb_socket:counter_incr(PrevPB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 3}, riakc_pb_socket:counter_val(PrevPB, ?BUCKET, ?KEY)),
    ?assertEqual(ok, riakc_pb_socket:counter_incr(PB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 4}, riakc_pb_socket:counter_val(PB, ?BUCKET, ?KEY)),
    pass.

get_clients(Node) ->
    {rt:pbc(Node), rt:httpc(Node)}.
