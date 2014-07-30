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
    %% Create a mixed cluster of legacy and previous
    %% Create a PB client
    %% GET  / PUT on older and newer cluster
    %% Upgrade nodes to previous
    %% Get put on all nodes
    Config = [],
    [Legacy, Previous] = Nodes = rt:build_cluster([{legacy, Config}, {previous, Config}]),
    ?assertEqual(ok, rt:wait_until_capability_contains(Previous, {riak_kv, crdt}, [pncounter])),
    verify_counter_converge:set_allow_mult_true(Nodes),

    {LegacyPB, LegacyHttp} = get_clients(Legacy),
    {PrevPB, PrevHttp} = get_clients(Previous),

    ?assertMatch(ok, rhc:counter_incr(LegacyHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 1}, rhc:counter_val(LegacyHttp, ?BUCKET, ?KEY)),

    ?assertMatch(ok, rhc:counter_incr(PrevHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 2}, rhc:counter_val(PrevHttp, ?BUCKET, ?KEY)),

    ?assertEqual(ok, riakc_pb_socket:counter_incr(LegacyPB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 3}, riakc_pb_socket:counter_val(LegacyPB, ?BUCKET, ?KEY)),
    ?assertEqual(ok, riakc_pb_socket:counter_incr(PrevPB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 4}, riakc_pb_socket:counter_val(PrevPB, ?BUCKET, ?KEY)),

    riakc_pb_socket:stop(LegacyPB),

    rt:upgrade(Legacy, previous),

    PrevPB2 = rt_pb:pbc(Legacy),

    ?assertEqual(ok, rt:wait_until_capability_contains(Previous, {riak_kv, crdt}, [pncounter,riak_dt_pncounter,riak_dt_orswot,riak_dt_map])),

    ?assertMatch(ok, rhc:counter_incr(LegacyHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 5}, rhc:counter_val(LegacyHttp, ?BUCKET, ?KEY)),

    ?assertMatch(ok, rhc:counter_incr(PrevHttp, ?BUCKET, ?KEY, 1)),
    ?assertMatch({ok, 6}, rhc:counter_val(PrevHttp, ?BUCKET, ?KEY)),

    ?assertEqual(ok, riakc_pb_socket:counter_incr(PrevPB2, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 7}, riakc_pb_socket:counter_val(PrevPB2, ?BUCKET, ?KEY)),
    ?assertEqual(ok, riakc_pb_socket:counter_incr(PrevPB, ?BUCKET, ?KEY, 1)),
    ?assertEqual({ok, 8}, riakc_pb_socket:counter_val(PrevPB, ?BUCKET, ?KEY)),

    [riakc_pb_socket:stop(C) || C <- [PrevPB, PrevPB2]],

    pass.

get_clients(Node) ->
    {rt_pb:pbc(Node), rt:httpc(Node)}.
