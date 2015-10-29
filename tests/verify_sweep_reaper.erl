%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%% @copyright (C) 2015, Basho Technologies
%%% @doc
%%% riak_test for riak_kv_sweeper and 
%%%
%%% Verify that the sweeper doesn't reap until we set a short grace period
%%%
%%% @end

-module(verify_sweep_reaper).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-define(NUM_NODES, 1).
-define(NUM_KEYS, 1000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(WAIT_FOR_SWEEP, 20000).
-define(TOMBSTONE_GRACE, 1).

confirm() ->
    Config = [{riak_core, 
               [{ring_creation_size, 4}
               ]},
              {riak_kv,
               [{delete_mode, keep},
                {reap_sweep_interval, 5},
                {sweep_tick, 3000},       %% Speed up sweeping
                {anti_entropy, {on, []}},
                {anti_entropy_tick, 24 * 60 * 60 * 1000}
               ]}
             ],

    [Node] = Nodes = rt:build_cluster(1, Config),
    [Client] = create_pb_clients(Nodes),

    
    KV1 = test_data(1, 1000),
    write_data(Client, KV1),
    delete_keys(Client, KV1),
    wait_for_sweep(),

    false = check_reaps(Node, Client, KV1),
    disable_sweep_scheduling(Nodes),
    set_tombstone_grace(Nodes, ?TOMBSTONE_GRACE),
    false = check_reaps(Node, Client, KV1),
    enable_sweep_scheduling(Nodes),

    %% Write key.
    KV2 = test_data(1001, 2000),
    write_data(Client, KV2),
    delete_keys(Client, KV2),
    wait_for_sweep(),
    true = check_reaps(Node, Client, KV2),
    true = check_reaps(Node, Client, KV1),

    %% Sweep after restart 
    disable_sweep_scheduling(Nodes),
    KV3 = test_data(2001, 2001),
    write_data(Client, KV3),
    delete_keys(Client, KV3),
    timer:sleep(?TOMBSTONE_GRACE * 1500),
    manually_sweep_all(Node),
    wait_for_sweep(),
    true = check_reaps(Node, Client, KV3),

    KV5 = test_data(3001, 4000),
    write_data(Client, KV5),
    delete_keys(Client, KV5),
    remove_sweep_participant(Nodes, riak_kv_delete),
    add_sweep_participant(Nodes),
    enable_sweep_scheduling(Nodes),
    wait_for_sweep(),
    true = check_reaps(Node, Client, KV5),

    KV6 = test_data(10001, 15000),
    write_data(Client, KV6),
    delete_keys(Client, KV6),
    timer:sleep(10000),
    manually_sweep_all(Node),
    remove_sweep_participant(Nodes, riak_kv_delete),
    add_sweep_participant(Nodes),
    enable_sweep_scheduling(Nodes),
    wait_for_sweep(),
    true = check_reaps(Node, Client, KV6),

    disable_sweep_scheduling(Nodes),
    KV7 = test_data(15001, 20000),
    write_data(Client, KV7),
    delete_keys(Client, KV7),
    timer:sleep(10000),
    manual_sweep(Node, 0),
    get_status(Node),
    timer:sleep(1000),
    get_status(Node),
    timer:sleep(1000),
    get_status(Node),
    timer:sleep(1000),
    get_status(Node),
    
    pass.

wait_for_sweep() ->
    wait_for_sweep(?WAIT_FOR_SWEEP).

wait_for_sweep(WaitTime) ->
    lager:info("Wait for sweep ~p s", [WaitTime]),
    timer:sleep(WaitTime).

write_data(Client, KVs) ->
    lager:info("Writing data ~p keys", [length(KVs)]),
    write_data(Client, KVs, []).
write_data(Client, KVs, Opts) ->
    [begin
         O = riakc_obj:new(?BUCKET, K, V),
         ?assertMatch(ok, riakc_pb_socket:put(Client, O, Opts))
     end || {K, V} <- KVs],
    ok.
test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

delete_keys(Client, KVs) ->
    lager:info("Delete data ~p keys", [length(KVs)]),
    [{delete_key(Client, K)}  || {K, _V} <- KVs].
delete_key(Client, Key) ->
    riakc_pb_socket:delete(Client, ?BUCKET, Key).

check_reaps(Node, Client, KVs) ->
    RR1 = get_read_repairs(Node),
    lager:info("Check data ~p keys", [length(KVs)]),
    Results = [check_reap(Client, K)|| {K, _V} <- KVs],
    Reaped = length([ true || true <- Results]),
    RR2 = get_read_repairs(Node),
    ReadRepaired = RR2-RR1,
    lager:info("Reaped ~p Read repaired ~p", [Reaped, ReadRepaired]),
    Reaped + ReadRepaired == length(KVs).

check_reap(Client, Key) ->
    case riakc_pb_socket:get(Client, ?BUCKET, Key, [deletedvclock]) of
        {error, notfound} ->
            true;
        _ ->
            false
    end.

%%% Client/Key ops
create_pb_clients(Nodes) ->
    [begin
         C = rt:pbc(N),
         riakc_pb_socket:set_options(C, [queue_if_disconnected]),
         C
     end || N <- Nodes].

set_tombstone_grace(Nodes, Time) ->
    lager:info("set_tombstone_grace ~p s ", [Time]),
    rpc:multicall(Nodes, application, set_env, [riak_kv, tombstone_grace_period,Time]).

%% set_sweep_throttle(Nodes, ThrottleLimit) ->
%%     lager:info("set_sweep_throttle ~p", [ThrottleLimit]),
%%     rpc:multicall(Nodes, application, set_env, [riak_kv, sweep_throttle,ThrottleLimit]).

disable_sweep_scheduling(Nodes) ->
    lager:info("disable sweep scheduling"),
    rpc:multicall(Nodes, riak_kv_sweeper, disable_sweep_scheduling, []).

enable_sweep_scheduling(Nodes) ->
    lager:info("enable sweep scheduling"),
    rpc:multicall(Nodes, riak_kv_sweeper, enable_sweep_scheduling, []).

remove_sweep_participant(Nodes, Module) ->
    lager:info("enable sweep scheduling"),
    {Succ, Fail} = rpc:multicall(Nodes, riak_kv_sweeper, remove_sweep_participant, [Module]),
    FalseResults = 
        [false || false <- Succ],
    0 = length(FalseResults) + length(Fail).

add_sweep_participant(Nodes) ->
    lager:info("add sweep participant"),
    rpc:multicall(Nodes, riak_kv_delete_sup, add_sweep_participant, []).

manually_sweep_all(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Indices = rpc:call(Node, riak_core_ring, my_indices, [Ring]),
    [begin manual_sweep(Node, Index), timer:sleep(5000)  end || Index <- Indices].

manual_sweep(Node, Partition) ->
    lager:info("Manual sweep index ~p", [Partition]),
   rpc:call(Node, riak_kv_sweeper, sweep, [Partition]).

get_read_repairs(Node) ->
    Stats = rpc:call(Node, riak_kv_status, get_stats, [console]),
    proplists:get_value(read_repairs_total, Stats).

%% wait_until_no_reaps(Nodes) ->
%%     lager:info("Wait until all reapes are done so we don't read repair them"),
%%     rt:wait_until(fun() -> no_aae_reaps(Nodes) end, 6, 10000).

no_aae_reaps(Nodes) when is_list(Nodes) ->
    MaxCount = max_aae_reaps(Nodes),
    lager:info("Max reaps across the board is ~p", [MaxCount]),
    MaxCount == 0.

max_aae_reaps(Nodes) when is_list(Nodes) ->
    MaxCount = lists:max([max_aae_reaps(Node) || Node <- Nodes]),
    MaxCount;
max_aae_reaps(Node) when is_atom(Node) ->
    Stats = rpc:call(Node, riak_kv_status, get_stats, [console]),
    proplists:get_value(vnode_reap_tombstone, Stats).

get_status(Node) ->
    Status = rpc:call(Node, riak_kv_sweeper, status, []),
    io:format(Status).

