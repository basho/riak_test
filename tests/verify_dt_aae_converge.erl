% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
%%% @copyright (C) 2014, Basho Technologies
%%% @doc
%%% riak_test for riak_dt CRDT convergence
%%% @end

-module(verify_dt_aae_converge).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(PB_BUCKET, <<"pbtest">>).
-define(KEY, <<"test">>).
-define(MODIFY_OPTS, [create]).
-define(TYPES, [{<<"counters">>, counter},
                {<<"sets">>, set},
                {<<"maps">>, map}]).
-define(READ_OPTS, [{r,2}, {notfound_ok, true}, {timeout, 5000}]).
-define(READ_ALL, [{r,3}, {timeout, 5000}]).
-define(N_VAL, 3).

-define(CFG,
    [{riak_kv,
        [
            % Speedy AAE configuration
            {anti_entropy, {on, []}},
            {anti_entropy_build_limit, {100, 1000}},
            {anti_entropy_concurrency, 100},
            {anti_entropy_expire, 24 * 60 * 60 * 1000}, % Not for now!
            {anti_entropy_tick, 500}
        ]},
    {riak_core,
        [
            {ring_creation_size, 16}
        ]}]).

confirm() ->
    Nodes = rt:build_cluster(4, ?CFG),
    create_bucket_types(Nodes, ?TYPES),
    RiakcPids = create_pb_clients(Nodes),
    assert_partitioned_writes_converge(Nodes, RiakcPids),
    assert_n1_writes_then_aae_repair_converges(Nodes, RiakcPids),
    assert_partition_loss_then_aae_repair_converges(Nodes, hd(RiakcPids)),
    pass.

assert_partitioned_writes_converge([N1, N2, N3, N4], [P1, P2, P3, _P4]) ->
    increment_counter(P1, 5),
    verify_counter(P2, 5),
    PartInfo = rt:partition([N1, N2], [N3, N4]),
    increment_counter(P1, 5),
    verify_counter(P2, 10, ?READ_ALL),
    verify_counter(P3, 5, ?READ_ALL),
    ok = rt:heal(PartInfo),
    verify_counter(P3, 10, ?READ_ALL),
    lager:info("Partitioned Writes Converged Correctly").

assert_n1_writes_then_aae_repair_converges(Nodes, RiakcPids) ->
    [P1 | _] = RiakcPids,
    increment_counter(P1, 5, [{n_val, 1} | ?MODIFY_OPTS]),
    PrimaryVal = get_replica_val(Nodes, 1),        
    ?assertEqual(PrimaryVal, 15),
    SecondaryVal = get_replica_val(Nodes, 2),
    ?assertEqual(SecondaryVal, 10),
    wait_for_aae_repair(Nodes, 15),
    %% Note there's a 50/50 chance this would pass without aae due to 
    %% read_repair
    verify_counter(P1, 15),
    lager:info("N=1 AAE Repaired Converged Correctly").

assert_partition_loss_then_aae_repair_converges([N1 | _]=Nodes, RiakcPid) ->
    Preflist = get_preflist(N1, {<<"counters">>, ?PB_BUCKET}, ?KEY),
    {Partition, Node}= hd(lists:reverse(Preflist)),
    wipe_out_partition(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    wait_for_aae_repair(Nodes, 15),
    verify_counter(RiakcPid, 15),
    lager:info("Successfully Repaired Lost Partition and Converged").

wipe_out_partition(Node, Partition) ->
    lager:info("Wiping out partition ~p in node ~p", [Partition, Node]),
    rt:clean_data_dir(Node, dir_for_partition(Partition)),
    ok.

restart_vnode(Node, Service, Partition) ->
    VNodeName = list_to_atom(atom_to_list(Service) ++ "_vnode"),
    {ok, Pid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                         [Partition, VNodeName]),
    ?assert(rpc:call(Node, erlang, exit, [Pid, kill_for_test])),
    Mon = monitor(process, Pid),
    receive
        {'DOWN', Mon, _, _, _} ->
            ok
    after
        rt_config:get(rt_max_wait_time) ->
            lager:error("VNode for partition ~p did not die, the bastard",
                        [Partition]),
            ?assertEqual(vnode_killed, {failed_to_kill_vnode, Partition})
    end,
    {ok, NewPid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                            [Partition, VNodeName]),
    lager:info("Vnode for partition ~p restarted as ~p",
               [Partition, NewPid]).

dir_for_partition(Partition) ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    BaseDir = base_dir_for_backend(KVBackend),
    filename:join([BaseDir, integer_to_list(Partition)]).

base_dir_for_backend(undefined) ->
    base_dir_for_backend(bitcask);
base_dir_for_backend(bitcask) ->
    "bitcask";
base_dir_for_backend(eleveldb) ->
    "leveldb".

get_preflist(Node, B, K) ->
    DocIdx = rpc:call(Node, riak_core_util, chash_key, [{B, K}]),
    PlTagged = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, ?N_VAL, riak_kv]),
    Pl = [E || {E, primary} <- PlTagged],
    Pl.

wait_for_aae_repair(Nodes, ExpectedVal) ->
    Delay = 2000, % every two seconds until max time.
    Retries = 100,
    CheckFun = fun() ->
                   Vals = [get_replica_val(Nodes, I) || I <- [1,2,3]],
                   [A, B, C] = Vals,
                   lager:info("Waiting for AAE Repair. Vals = ~p, Expected = ~p"
                       , [Vals, ExpectedVal]),
                   A =:= B andalso B =:= C andalso C =:= ExpectedVal
               end,
    case rt:wait_until(CheckFun, Retries, Delay) of
        ok ->
            lager:info("Data is now correct. Yay!");
        fail ->
            lager:error("AAE failed to fix data"),
            ?assertEqual(aae_fixed_data, aae_failed_to_fix_data)
    end.

get_replica_val(Nodes, I) ->
    Node1 = hd(Nodes),
    Bucket = {<<"counters">>, ?PB_BUCKET},
    case rt:get_replica(Node1, Bucket, ?KEY, I, ?N_VAL) of
        {ok, Obj} ->
            counter_value(Node1, Obj);
        {error, notfound} ->
            -99999999
    end.

%% just use an rpc since that beam isn't available on the riak_test side
counter_value(Node, Obj) ->
    rpc:call(Node, riak_kv_crdt, counter_value, [Obj]).

increment_counter(RiakcPid, Value) ->
    increment_counter(RiakcPid, Value, ?MODIFY_OPTS).

increment_counter(RiakcPid, Value, Opts) ->
    riakc_pb_socket:modify_type(RiakcPid, make_increment(Value), 
        {<<"counters">>, ?PB_BUCKET}, ?KEY, Opts). 

make_increment(Value) ->
    fun(C) ->
        riakc_counter:increment(Value, C)
    end.

verify_counter(RiakcPid, Expected) ->
    verify_counter(RiakcPid, Expected, ?READ_OPTS).

verify_counter(RiakcPid, Expected, ReadOpts) ->
    {ok, Val} = riakc_pb_socket:fetch_type(RiakcPid, 
        {<<"counters">>, ?PB_BUCKET}, ?KEY, ReadOpts),
    ?assertEqual(Expected, riakc_counter:value(Val)).

create_pb_clients(Nodes) ->
    [rt:pbc(N) || N <- Nodes].

create_bucket_types([N1|_]=Nodes, Types) ->
    lager:info("Creating bucket types with datatypes: ~p", [Types]),
    [rpc:call(N1, riak_core_bucket_type, create,
               [Name, [{datatype, Type}, {allow_mult, true}]]) ||
        {Name, Type} <- Types ],
    [rt:wait_until(N1, bucket_type_ready_fun(Name)) || {Name, _Type} <- Types],
    [rt:wait_until(N, bucket_type_matches_fun(Types)) || N <- Nodes].

bucket_type_ready_fun(Name) ->
    fun(Node) ->
            Res = rpc:call(Node, riak_core_bucket_type, activate, [Name]),
            lager:info("is ~p ready ~p?", [Name, Res]),
            Res == ok
    end.

bucket_type_matches_fun(Types) ->
    fun(Node) ->
            lists:all(fun({Name, Type}) ->
                              Props = rpc:call(Node, riak_core_bucket_type, get,
                                               [Name]),
                              Props /= undefined andalso
                                  proplists:get_value(allow_mult, Props, false)
                                  andalso
                                  proplists:get_value(datatype, Props) == Type
                      end, Types)
    end.

