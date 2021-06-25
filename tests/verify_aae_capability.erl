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
%% @doc Verification of Active Anti Entropy Bakckground Manager.

-module(verify_aae_capability).
-export([confirm/0, check_capability/3, check_objversion/3]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(AAE_THROTTLE_LIMITS, [{-1, 0}, {100, 10}]).
-define(TICK_TIME, 1000).
-define(LOOP_TIME, 100).
-define(CFG,
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {on, []}},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 10},
           {anti_entropy_expire, 24 * 60 * 60 * 1000}, % Not for now!
           {anti_entropy_tick, ?TICK_TIME},
           {aae_throttle_limits, ?AAE_THROTTLE_LIMITS},
           {aae_use_background_manager, true},
           {override_capability, [{object_hash_version, [{use, 0}]}]}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE},
           {capability_tick, 100}
          ]}]
       ).
-define(NUM_NODES, 4).
-define(NUM_KEYS, 100000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(CHECK_TYPE, check_capability).

confirm() ->
    Nodes = rt:build_cluster(?NUM_NODES, ?CFG),
    verify_bg_mgr(Nodes),
    pass.


verify_bg_mgr(Nodes) ->
    [Node1, Node2, Node3, Node4] = Nodes,

    % Verify that AAE eventually upgrades to version 0 (or already has)
    verify_aae:wait_until_hashtree_upgrade(Nodes),

    lager:info("Writing ~w objects", [?NUM_KEYS]),
    KV1 = test_data(1, ?NUM_KEYS),
    write_data(Node1, KV1),

    ok = restart_nodes([Node1, Node2, Node3, Node4], 1),
    ok = restart_nodes([Node1, Node2, Node3, Node4], 10),
    ok = restart_nodes([Node1, Node2, Node3, Node4], 100),
    ok = restart_nodes([Node1, Node2, Node3, Node4], 1000),
    ok = restart_nodes([Node1, Node2, Node3, Node4], 10000),
    
    ok.

restart_nodes(Nodes, RandTimer) ->
    lager:info("Restarting nodes with rand gaps of up to ~w ms", [RandTimer]),
    [Node1, Node2, Node3, Node4] = Nodes,
    lager:info("Stop all nodes"),
    lists:foreach(fun(N) -> rt:stop_and_wait(N) end, Nodes),

    Self = self(),
    
    lager:info("Run ~w while starting all nodes", [?CHECK_TYPE]),
    spawn(?MODULE, ?CHECK_TYPE, [Node1, Self, 30000 div ?LOOP_TIME]),
    spawn(?MODULE, ?CHECK_TYPE, [Node2, Self, 30000 div ?LOOP_TIME]),
    spawn(?MODULE, ?CHECK_TYPE, [Node3, Self, 30000 div ?LOOP_TIME]),
    spawn(?MODULE, ?CHECK_TYPE, [Node4, Self, 30000 div ?LOOP_TIME]),
    spawn(rt, start, [Node1]),
    timer:sleep(rand:uniform(RandTimer)),
    spawn(rt, start, [Node2]),
    timer:sleep(rand:uniform(RandTimer)),
    spawn(rt, start, [Node3]),
    timer:sleep(rand:uniform(RandTimer)),
    spawn(rt, start, [Node4]),
    ok = receive R1 -> R1 end,
    ok = receive R2 -> R2 end,
    ok = receive R3 -> R3 end,
    ok = receive R4 -> R4 end,

    ok.

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, ?BUCKET, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(?BUCKET, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.

check_capability(Node, Tester, N) ->
    check_capability(Node, Tester, N, []).

check_capability(_Node, Tester, 0, []) ->
    Tester ! ok;
check_capability(_Node, Tester, 0, ErrorList) ->
    Tester ! ErrorList;
check_capability(Node, Tester, N, ErrorList) ->
    EMPid =
        riak_core_util:safe_rpc(Node,
            erlang,
            whereis,
            [riak_kv_entropy_manager]),
    IsAlive = 
        riak_core_util:safe_rpc(Node,
            erlang,
            is_process_alive,
            [EMPid]),
    Vers =
        riak_core_util:safe_rpc(Node,
            riak_core_capability,
            get,
            [{riak_kv, object_hash_version}, legacy]),
    case {IsAlive, Vers} of
        {true, 0} ->
            timer:sleep(?LOOP_TIME),
            check_capability(Node, Tester, N - 1, ErrorList);
        {true, {badrpc, nodedown}} ->
            timer:sleep(?LOOP_TIME),
            check_capability(Node, Tester, N - 1, ErrorList);
        {true, _} ->
            timer:sleep(?LOOP_TIME),
            lager:info("Unexpected capability check ~p on ~p", [Vers, Node]),
            check_capability(Node, Tester, N - 1, [Vers|ErrorList]);
        _ ->
            timer:sleep(?LOOP_TIME),
            check_capability(Node, Tester, N - 1, ErrorList)
    end.


check_objversion(Node, Tester, N) ->
    check_objversion(Node, Tester, N, []).

check_objversion(_Node, Tester, 0, []) ->
    Tester ! ok;
check_objversion(_Node, Tester, 0, ErrorList) ->
    Tester ! ErrorList;
check_objversion(Node, Tester, N, ErrorList) ->
    Vers =
        riak_core_util:safe_rpc(Node,
            application,
            get_env,
            [riak_kv, object_hash_version]),
    case Vers of
        {ok, 0} ->
            timer:sleep(?LOOP_TIME),
            check_objversion(Node, Tester, N - 1, ErrorList);
        {badrpc, nodedown} ->
            timer:sleep(?LOOP_TIME),
            check_objversion(Node, Tester, N - 1, ErrorList);
        _ ->
            timer:sleep(?LOOP_TIME),
            lager:info("Unexpected object version ~p on ~p", [Vers, Node]),
            check_objversion(Node, Tester, N - 1, [Vers|ErrorList])
    end.
