-module(sweeper_bench).
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
%% @doc The large scale test is to test:
%%
%% Properties
%%
%% large scale
%% real time balance working
%% fullsync not halted by nodes up/down/add/remove
%% realtime not halted by nodes up/down/add/remove
%% Sweeper AAE tree rebuild and Sweeper reaper
-behavior(riak_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-export([confirm/0]).

-define(DEFAULT_BENCH_DURATION, 15).

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    
    Tests = [
             {"50%_ttl_sweep", [{put_ttl, 5}, {put, 5}], true},
             {"70%_ttl_sweep", [{put_ttl, 7}, {put, 3}], true},
             {"100%_ttl_sweep", [{put_ttl, 1}], true},
             {"no_ttl_no_sweep", [{put, 1}], false},
             {"no_ttl_sweep", [{put, 1}], true},
             {"10%_ttl_sweep", [{put_ttl, 1}, {put, 9}], true}],
    
    
    [bench(Name, BenchAction, Sweep) || {Name, BenchAction, Sweep } <- Tests],
    pass.

bench(Name, BenchAction, Sweep) ->
    Config = [
              {riak_core, 
               [{ring_creation_size, 8}
               ]},
              {riak_kv,
               [{storage_backend, riak_kv_eleveldb_backend},
                {delete_mode, keep},
                {tombstone_grace_period, 3600}, %% 1h in s
                {reap_sweep_interval, 900},
                {obj_ttl_sweep_interval, 900},
                {anti_entropy_expire, 3600 * 1000},
                {sweep_tick, 5000},       %% Speed up sweeping
                {anti_entropy_build_limit, {100, 1000}},
                {anti_entropy, {on, [debug]}},
                {anti_entropy_tick, 5000}
               ]}
             ],

    [Node] = Nodes = rt:build_cluster(1, Config),
    disable_sweep_scheduling(Nodes),
    lager:info("Start ttl loading bench"),
    start_basho_bench(Nodes, "putttl", BenchAction),
    
    BenchDuration =
        rt_config:get(basho_bench_duration, ?DEFAULT_BENCH_DURATION),
    timer:sleep(timer:minutes(trunc(BenchDuration) + 2)),
    lager:info("Start normal bench"),
    
     start_basho_bench(Nodes, Name, [{put, 1}, 
                                     {update, 1}, 
                                     {delete, 1}]),

    timer:sleep(timer:minutes(trunc(BenchDuration/2))),
    case Sweep of
        true ->
            manual_sweep(Node, 0);
        false ->
            ok
    end,
    
    timer:sleep(timer:minutes(trunc(BenchDuration/2) + 2)),
    
    rt:clean_cluster(Nodes).

get_status(Node) ->
    rpc:call(Node, riak_kv_console, sweep_status, [[]]).


start_basho_bench(Nodes, Name, Operations) ->
    PbIps = lists:map(fun(Node) ->
                              {ok, [{PB_IP, PB_Port}]} = rt:get_pb_conn_info(Node),
                              {PB_IP, PB_Port}
                      end, Nodes),

    LoadConfig = bacho_bench_config(PbIps, Operations),
    spawn(fun() -> rt_bench:bench(LoadConfig, Nodes, Name, 1, false) end).


bacho_bench_config(HostList, Operations) ->
    BenchRate =
        rt_config:get(basho_bench_rate, 300),
    BenchDuration =
        rt_config:get(basho_bench_duration, ?DEFAULT_BENCH_DURATION),
    KeyGen =
        rt_config:get(basho_bench_keygen, {int_to_bin_bigendian, {partitioned_sequential_int, 1000000000}}),
    ValGen =
        rt_config:get(basho_bench_valgen, {exponential_bin, 1000, 5000}),
    %% {get, 1},{put, 1},{delete, 2}
    Operations =
        rt_config:get(basho_bench_operations, Operations),
    Bucket =
        rt_config:get(basho_bench_bucket, <<"mybucket">>),
    Driver =
        rt_config:get(basho_bench_driver, riakc_pb),
    ObjTTL =
        rt_config:get(obj_ttl, 5),

    rt_bench:config(BenchRate,
                    BenchDuration,
                    HostList,
                    KeyGen,
                    ValGen,
                    Operations,
                    Bucket,
                    Driver,
                    [{obj_ttl, ObjTTL}, {report_interval, 1}]).


disable_sweep_scheduling(Nodes) ->
    lager:info("disable sweep scheduling"),
    {Succ, Fail} = rpc:multicall(Nodes, riak_kv_sweeper, stop_all_sweeps, []),
    BadResults = [Res || Res <- Succ, not is_integer(Res)],
    ?assertEqual([], BadResults),
    ?assertEqual([], Fail).

%% enable_sweep_scheduling(Nodes) ->
%%     lager:info("enable sweep scheduling"),
%%     rpc:multicall(Nodes, riak_kv_sweeper, enable_sweep_scheduling, []).

manual_sweep(Node, Partition) ->
   lager:info("Manual sweep index ~p", [Partition]),
   rpc:call(Node, riak_kv_sweeper, sweep, [Partition]).
