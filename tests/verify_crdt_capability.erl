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
%%% riak_test for crdt cabability
%%% @end

-module(verify_crdt_capability).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test-counters">>).
-define(KEY, <<"foo">>).

confirm() ->
    %% Create a mixed cluster of current and previous versions
    %% Create a PB client
    %% GET  / PUT on current and previous cluster
    %% Upgrade nodes
    %% Get put on all nodes
    Config = [],
    [Previous, Current]=Nodes = rt:build_cluster([{previous, Config}, {current, Config}]),
    ?assertEqual(ok, rt:wait_until_capability(Current, {riak_kv, crdt}, [pncounter,riak_dt_pncounter,riak_dt_orswot,riak_dt_map])),

    verify_counter_converge:set_allow_mult_true(Nodes),

    {PrevPB, PrevHttp} = get_clients(Previous),
    {PB, Http} = get_clients(Current),

    ?assertMatch(ok, rhc:counter_incr(PrevHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 1}, rhc:counter_val(PrevHttp, ?BUCKET, ?KEY)),

    ?assertMatch(ok, rhc:counter_incr(Http, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 2}, rhc:counter_val(Http, ?BUCKET, ?KEY)),

    ?assertEqual(ok, riakc_pb_socket:counter_incr(PrevPB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 3}, riakc_pb_socket:counter_val(PrevPB, ?BUCKET, ?KEY)),
    ?assertEqual(ok, riakc_pb_socket:counter_incr(PB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 4}, riakc_pb_socket:counter_val(PB, ?BUCKET, ?KEY)),

    lager:info("Passed mixed test, upgrade time!"),

    rt:upgrade(Previous, current),
    lager:info("Upgrayded!!"),
    ?assertEqual(ok, rt:wait_until_capability(Current, {riak_kv, crdt}, [riak_dt_pncounter, riak_dt_orswot, riak_dt_map, pncounter])),

    ?assertMatch(ok, rhc:counter_incr(PrevHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 5}, rhc:counter_val(PrevHttp, ?BUCKET, ?KEY)),

    ?assertMatch(ok, rhc:counter_incr(Http, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 6}, rhc:counter_val(Http, ?BUCKET, ?KEY)),

    %% Reconnect to the upgraded node.
    riakc_pb_socket:stop(PrevPB),
    {PrevPB1, _} = get_clients(Previous),

    ?assertEqual(ok, riakc_pb_socket:counter_incr(PrevPB1, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 7}, riakc_pb_socket:counter_val(PrevPB1, ?BUCKET, ?KEY)),
    ?assertEqual(ok, riakc_pb_socket:counter_incr(PB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 8}, riakc_pb_socket:counter_val(PB, ?BUCKET, ?KEY)),

    %% And check that those 1.4 written values can be accessed /
    %% incremented over the 2.0 API

    ?assertEqual(8, begin
                        {ok, Counter} = riakc_pb_socket:fetch_type(PrevPB1, {<<"default">>, ?BUCKET}, ?KEY),
                        riakc_counter:value(Counter)
                    end),    
    ?assertEqual(ok, riakc_pb_socket:update_type(PrevPB1, {<<"default">>, ?BUCKET}, ?KEY, gen_counter_op())),
    ?assertEqual({ok, 9}, riakc_pb_socket:counter_val(PB, ?BUCKET, ?KEY)),

    [riakc_pb_socket:stop(C) || C <- [PB, PrevPB1]],

    pass.

gen_counter_op() ->
    riakc_counter:to_op(riakc_counter:increment(riakc_counter:new())).

get_clients(Node) ->
    {rt:pbc(Node), rt:httpc(Node)}.
