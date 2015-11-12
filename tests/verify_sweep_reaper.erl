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
%% -compile(export_all).
-define(NUM_NODES, 1).
-define(NUM_KEYS, 1000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(SWEEP_TICK, 1000).
-define(WAIT_FOR_SWEEP, ?SWEEP_TICK * 6).
-define(SHORT_TOMBSTONE_GRACE, 1).
-define(LONG_TOMBSTONE_GRACE, 1000).

confirm() ->
    Config = [{riak_core, 
               [{ring_creation_size, 4}
               ]},
              {riak_kv,
               [{delete_mode, keep},
                {tombstone_grace_period, 7 * 24 * 3600}, %% 1w in s
                {reap_sweep_interval, 1},
                {sweep_tick, ?SWEEP_TICK},       %% Speed up sweeping
                {anti_entropy_build_limit, {100, 1000}},
                {anti_entropy_concurrency, 10},
                {anti_entropy, {on, [debug]}},
                {anti_entropy_tick, 2000}
               ]}
             ],

    [Node] = Nodes = rt:build_cluster(1, Config),
    [Client] = create_pb_clients(Nodes),

    KV1 = test_data(1, 100),
    verify_no_reap(Nodes, KV1),

    set_tombstone_grace(Nodes, ?SHORT_TOMBSTONE_GRACE),
    KV2 = test_data(101, 200),
    verify_reap(Nodes, KV1, KV2),

    KV3 = test_data(201, 201),
    verify_manual_sweep(Nodes, KV3),

    KV4 = test_data(301, 400),
    verify_remove_add_participant(Nodes, KV4),

    KV6 = test_data(1001, 1500),
    write_data(Client, KV6),
    delete_keys(Client, KV6),
    timer:sleep(10000),
    manually_sweep_all(Node),
    remove_sweep_participant(Nodes, riak_kv_delete),
    add_sweep_participant(Nodes),
    enable_sweep_scheduling(Nodes),
    wait_for_sweep(),
    true = check_reaps(Node, Client, KV6),

    KV7 = test_data(1501, 1600), %% AAE repair write {n_val, 1}
    KV8 = test_data(1601, 1700), %% AAE repair delete {n_val, 1} then reap
    KV9 = test_data(1701, 1800), %% AAE no repair then reap {n_val, 1}

    verify_aae_and_reaper_interaction(Nodes, KV7, KV8, KV9),

    verify_scheduling(Nodes),

    KV10 = test_data(10001, 30000),
    test_status(Nodes, KV10),

    pass.

verify_no_reap([Node|_] = Nodes, KV) ->
    format_subtest(verify_no_reap),
    Client = rt:pbc(Node),
    write_data(Client, KV),
    delete_keys(Client, KV),
    wait_for_sweep(),

    false = check_reaps(Node, Client, KV),
    disable_sweep_scheduling(Nodes),
    false = check_reaps(Node, Client, KV),
    enable_sweep_scheduling(Nodes),
    riakc_pb_socket:stop(Client).

verify_reap([Node|_] = _Nodes, KV1, KV2) ->
    format_subtest(verify_reap),
    Client = rt:pbc(Node),
    write_data(Client, KV2),
    delete_keys(Client, KV2),
    wait_for_sweep(),
    true = check_reaps(Node, Client, KV2),
    true = check_reaps(Node, Client, KV1),
    riakc_pb_socket:stop(Client).

verify_manual_sweep([Node|_] = Nodes, KV) ->
    format_subtest(verify_manual_sweep),
    Client = rt:pbc(Node),
    disable_sweep_scheduling(Nodes),
    write_data(Client, KV),
    delete_keys(Client, KV),
    timer:sleep(?SHORT_TOMBSTONE_GRACE * 1500),
    manually_sweep_all(Node),
    true = check_reaps(Node, Client, KV),
    enable_sweep_scheduling(Nodes),
    riakc_pb_socket:stop(Client).

verify_remove_add_participant([Node|_] = Nodes, KV) ->
    format_subtest(verify_remove_add_participant),
    Client = rt:pbc(Node),
    remove_sweep_participant(Nodes, riak_kv_delete),
    write_data(Client, KV),
    delete_keys(Client, KV),
    wait_for_sweep(),
    false = check_reaps(Node, Client, KV),
    add_sweep_participant(Nodes),
    wait_for_sweep(),
    true = check_reaps(Node, Client, KV),
    riakc_pb_socket:stop(Client).

verify_aae_and_reaper_interaction([Node|_] = Nodes, KV1, KV2, KV3) ->
    format_subtest(verify_aae_in_grace),
    Client = rt:pbc(Node),
    disable_sweep_scheduling(Nodes),
    set_tombstone_grace(Nodes, ?LONG_TOMBSTONE_GRACE),
    write_data(Client, KV1, [{n_val, 1}]),
    true = verify_data(Node, KV1, changed),

    format_subtest(verify_aae_repair_tombstone),
    write_data(Client, KV2),
    delete_keys(Client, KV2, [{n_val, 1}]),
    true = verify_data(Node, KV2, delete, 30000),
    set_tombstone_grace(Nodes, ?SHORT_TOMBSTONE_GRACE),
    manually_sweep_all(Node),
    true = check_reaps(Node, Client, KV2),


    format_subtest(verify_aae_no_repair_tombstone),
    disable_aae(Node),
    write_data(Client, KV3),
    delete_keys(Client, KV3, [{n_val, 1}]),
    timer:sleep(?SHORT_TOMBSTONE_GRACE * 5000),
    enable_aae(Node),
    rt:wait_until_aae_trees_built(Nodes),
    false = verify_data(Node, KV3, delete, 30000),
    manually_sweep_all(Node),
    false = check_reaps(Node, Client, KV3),

    riakc_pb_socket:stop(Client).

verify_scheduling([Node|_] = Nodes) ->
    format_subtest(verify_scheduling),
    disable_sweep_scheduling(Nodes),
    %% First manually sweep then scheduled sweeps
    %% should be in same order
    Indices = manually_sweep_all(Node),
    enable_sweep_scheduling(Nodes),

    timer:sleep(?SWEEP_TICK * length(Indices)),
    {_Participants , Sweeps} = get_unformated_status(Node),
    ScheduledIndices =
        [element(2, Sweep) || Sweep <- lists:keysort(8, Sweeps)],
    Indices = ScheduledIndices,

    timer:sleep(10000),
    %% Test reversed
    disable_sweep_scheduling(Nodes),
    [begin manual_sweep(Node, Index), timer:sleep(1000) end ||
      Index <- lists:reverse(Indices)],
     enable_sweep_scheduling(Nodes),

    timer:sleep(?SWEEP_TICK * length(Indices)),
    {_Participants , ReverseSweeps} = get_unformated_status(Node),
    ReverseScheduledIndices =
        [element(2, Sweep) || Sweep <- lists:keysort(8, ReverseSweeps)],
    ReverseScheduledIndices = lists:reverse(Indices).

test_status([Node|_] = _Nodes, KV) ->
    format_subtest(test_status),
    Client = rt:pbc(Node),
    write_data(Client, KV),
    delete_keys(Client, KV),
    timer:sleep(10000),
    manual_sweep(Node, 0),
    get_status(Node),
    timer:sleep(1000),
    get_status(Node),
    timer:sleep(1000),
    get_status(Node),
    timer:sleep(1000),
    get_status(Node).

enable_aae(Node) ->
    lager:info("enable aae", []),
    rpc:call(Node, riak_kv_entropy_manager, enable, []).

disable_aae(Node) ->
    lager:info("disable aae", []),
    rpc:call(Node, riak_kv_entropy_manager, disable, []).


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
    delete_keys(Client, KVs, []).

delete_keys(Client, KVs, Opt) ->
    lager:info("Delete data ~p keys", [length(KVs)]),
    [{delete_key(Client, K, Opt)}  || {K, _V} <- KVs].

delete_key(Client, Key, Opt) ->
    {ok, Obj} = riakc_pb_socket:get(Client, ?BUCKET, Key),
    riakc_pb_socket:delete_obj(Client, Obj, Opt).

check_reaps(Node, Client, KVs) ->
    RR1 = get_read_repairs(Node),
    lager:info("Check data ~p keys", [length(KVs)]),
    Results = [check_reap(Client, K)|| {K, _V} <- KVs],
    Reaped = length([ true || true <- Results]),
    RR2 = get_read_repairs(Node),
    ReadRepaired = RR2-RR1,
    lager:info("Reaped ~p Read repaired ~p", [Reaped, ReadRepaired]),
    Reaped == length(KVs).

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

disable_sweep_scheduling(Nodes) ->
    lager:info("disable sweep scheduling"),
    {Succ, Fail} = rpc:multicall(Nodes, riak_kv_sweeper, disable_sweep_scheduling, []),
    FalseResults = 
        [false || false <- Succ],
    0 = length(FalseResults) + length(Fail).

enable_sweep_scheduling(Nodes) ->
    lager:info("enable sweep scheduling"),
    rpc:multicall(Nodes, riak_kv_sweeper, enable_sweep_scheduling, []).

remove_sweep_participant(Nodes, Module) ->
    lager:info("remove sweep participant"),
    {Succ, Fail} = rpc:multicall(Nodes, riak_kv_sweeper, remove_sweep_participant, [Module]),
    FalseResults = 
        [false || false <- Succ],
    0 = length(FalseResults) + length(Fail).


add_sweep_participant(Nodes) ->
    lager:info("add sweep participant"),
    rpc:multicall(Nodes, riak_kv_delete_sup, maybe_add_sweep_participant, []).

manually_sweep_all(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Indices = rpc:call(Node, riak_core_ring, my_indices, [Ring]),
    [begin manual_sweep(Node, Index), timer:sleep(1000), Index  end || Index <- Indices].

manual_sweep(Node, Partition) ->
   lager:info("Manual sweep index ~p", [Partition]),
   rpc:call(Node, riak_kv_sweeper, sweep, [Partition]).

get_read_repairs(Node) ->
    Stats = rpc:call(Node, riak_kv_status, get_stats, [console]),
    proplists:get_value(read_repairs_total, Stats).

get_status(Node) ->
    rpc:call(Node, riak_kv_console, sweep_status, [[]]).

get_unformated_status(Node) ->
    rpc:call(Node, riak_kv_sweeper, status, []).

% @doc Verifies that the data is eventually restored to the expected set.
verify_data(Node, KeyValues, Mode) ->
    MaxTime = rt_config:get(rt_max_wait_time),
    verify_data(Node, KeyValues, Mode, MaxTime).

verify_data(Node, KeyValues, Mode, MaxTime) ->
    lager:info("Verify all replicas are eventually correct"),
    PB = rt:pbc(Node),
    CheckFun =
        fun() ->
                Matches = [verify_replicas(Node, ?BUCKET, K, V, ?N_VAL, Mode)
                             || {K, V} <- KeyValues],
                CountTrues = fun(true, G) -> G+1; (false, G) -> G end,
                NumGood = lists:foldl(CountTrues, 0, Matches),
                Num = length(KeyValues),
                case Num == NumGood of
                    true -> true;
                    false ->
                        lager:info("Data not yet correct: ~p mismatches",
                                   [Num-NumGood]),
                        false
                end
        end,
    Delay = 2000, % every two seconds until max time.
    Retry = MaxTime div Delay,
    Response =
        case rt:wait_until(CheckFun, Retry, Delay) of
            ok ->
                lager:info("Data is now correct. Yay!"),
                true;
            _ ->
                lager:error("AAE failed to fix data"),
                false
        end,
    riakc_pb_socket:stop(PB),
    Response.

merge_values(O) ->
    Vals = riak_object:get_values(O),
    lists:foldl(fun(NV, V) ->
                        case size(NV) > size(V) of
                            true -> NV;
                            _ -> V
                        end
                end, <<>>, Vals).

verify_replicas(Node, B, K, _V, N, delete) ->
    Replies = [rt:get_replica(Node, B, K, I, N)
                 || I <- lists:seq(1,N)],
    Match = hd(Replies),
    length([del || Response <- Replies, Match == Response]) == N;

verify_replicas(Node, B, K, V, N, _Mode) ->
    Replies = [rt:get_replica(Node, B, K, I, N)
               || I <- lists:seq(1,N)],
    Vals = [merge_values(O) || {ok, O} <- Replies],
    Expected = [V || _ <- lists:seq(1, N)],
    Vals == Expected.

format_subtest(Test) ->
    TestString = atom_to_list(Test),
    lager:info("~s", [string:centre(" " ++ TestString ++ " " , 79, $=)]).
