-module(consistent_get_put).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    HostList = rt_config:get(rt_hostnames),
    Count = length(HostList),
    
    rtperf:standard_config(Count),

    ok = build_cluster(Config),

    %%[Node | _] = Nodes,
    %%rt:create_and_activate_bucket_type(Node, <<"sc">>, [{consistent, true}]),
    
    ok = rtperf:maybe_prepop(Hosts),

    %% this should also get moved into the perf harness.


    %% this needs to be broken apart and abstracted so it's easy to
    %% specify what needs to happen.  also need to add baseline load
    %% vs. test load.

    Test =
        case rt_config:get(perf_config, undefined) of
            undefined ->
                case rt_config:get(perf_test_type, undefined) of
                    undefined ->
                        error("no run config or runtype defined");
                    pareto ->
                        do_pareto;
                    uniform ->
                        do_uniform
                end
        end,


    %% this also

    Collectors = rtperf:start_data_collectors(HostList),

    %% need to come up with a better naming scheme, for sure

    TestName = rtperf:test_name(),
        
    rtperf:Test(HostList, BinSize, TestName),

    ok = rtperf:stop_data_collectors(Collectors),
    ok = rtperf:collect_test_data(HostList, TestName).


code_paths() ->
    {code_paths, ["evan/basho_bench/deps/riakc",
                  "evan/basho_bench/deps/riak_pb",
                  "evan/basho_bench/deps/protobuffs"]}.    

do_prepop(NodeList, BinSize, TestName) ->
    PrepopCount = rt_config:get(perf_prepop_size),
    Config = prepop_config(BinSize, NodeList, PrepopCount),
    lager:info("Config ~p", [Config]),
    rt_bench:bench(Config, NodeList, TestName),
    lager:info("Prepop complete").

prepop_config(BinSize, NodeList, BinCount0) ->
    Count = length(NodeList),
    {Mode, _Duration} = get_md(BinSize),

    BinCount = adjust_count(BinCount0, 30*Count),


    lager:info("Starting prepop run for ~p binaries of size ~p bytes",
               [BinCount, BinSize]),

    [Mode,
     %% run to completion
     {duration, infinity},
     {concurrent, 30*Count},
     {rng_seed, now},

     {riakc_pb_bucket, <<"b1">>},
     %%{riakc_pb_bucket, {<<"sc">>, <<"b1">>}},
     {key_generator, {int_to_bin, {partitioned_sequential_int, 0, BinCount}}},
     {value_generator, valgen(BinSize)},
     {operations, operations(prepop)},
     {sequential_int_state_dir, rt_bench:seq_state_dir()},

     %% should add asis when it's supported by the driver.
     {riakc_pb_ips, NodeList},
     {riakc_pb_replies, default},
     {driver, basho_bench_driver_riakc_pb},
     code_paths()].

adjust_count(Count, Concurrency) ->
    case Count rem Concurrency of
        0 -> Count;
        N -> Count + (Concurrency - N)
    end.

do_pareto(NodeList, BinSize, TestName) ->
    Config = pareto_config(BinSize, NodeList),
    lager:info("Config ~p", [Config]),
    rt_bench:bench(Config, NodeList, TestName, 2).

pareto_config(BinSize, NodeList) ->
    Count = length(NodeList),
    {Mode, Duration} = get_md(BinSize),

    [Mode,
     {duration, Duration},
     {concurrent, 30*Count},
     {rng_seed, now},

     {riakc_pb_bucket, <<"b1">>},
     {key_generator, {int_to_bin, {truncated_pareto_int, 10000000}}},
     {value_generator, valgen(BinSize)},
     {operations, operations(pareto)}, %% update - 50% get, 50% put

     %% should add asis when it's supported by the driver.
     {riakc_pb_ips, NodeList},
     {riakc_pb_replies, default},
     {driver, basho_bench_driver_riakc_pb},
     code_paths()].

do_uniform(NodeList, BinSize, TestName) ->
    Config = uniform_config(BinSize, NodeList),
    lager:info("Config ~p", [Config]),
    rt_bench:bench(Config, NodeList, TestName, 2).

uniform_config(BinSize, NodeList) ->
    Count = length(NodeList),
    {Mode, Duration} = get_md(BinSize),

    Numkeys =
    case rt_config:get(perf_prepop_size) of
        Count when is_integer(Count) ->
        Count;
        _ -> 10000000
    end,

    [Mode,
     {duration, Duration},
     {concurrent, 40*Count},
     {rng_seed, now},

     {riakc_pb_bucket, <<"b1">>},
     %%{riakc_pb_bucket, {<<"sc">>, <<"b1">>}},
     {key_generator, {int_to_bin, {uniform_int, Numkeys}}},
     {value_generator, valgen(BinSize)},
     {operations, operations(uniform)},

     %% should add asis when it's supported by the driver.
     {riakc_pb_ips, NodeList},
     {riakc_pb_replies, default},
     {driver, basho_bench_driver_riakc_pb},
     code_paths()].


get_md(BinSize) ->
    case rt_config:get(rt_backend, undefined) of
        %% hueristically determined nonsense, need a real model
        riak_kv_eleveldb_backend ->
            lager:info("leveldb"),
            Rate =
                case BinSize >= 10000 of
                    true -> maybe_override(50);
                    false -> maybe_override(75)
                end,
            {{mode, {rate, Rate}}, maybe_override(150)};
        _ ->
            %%fixme yo
            lager:info("unset or bitcask"),
            {{mode, max}, maybe_override(90)}
    end.

maybe_override(Default) ->
    case rt_config:get(perf_runtime, undefined) of
        N when is_integer(N) ->
            N;
        _ ->
            Default
    end.

date_string() ->
    {YrMoDay, HrMinSec} = calendar:local_time(),
    string:join(lists:map(fun erlang:integer_to_list/1,
                          tuple_to_list(YrMoDay)++tuple_to_list(HrMinSec)),
                "-").

valgen(BinSize) ->
    Type = rt_config:get(perf_bin_type),
    case Type of
        fixed ->
            {fixed_bin, BinSize};
        exponential ->
            Quarter = BinSize div 4,
            {exponential_bin, Quarter, Quarter*3}
    end.

operations(Type) ->
    LoadType = rt_config:get(perf_load_type),
    N =
        case LoadType of
            read_heavy -> 4;
            write_heavy -> 1
        end,

    case Type of
        prepop ->
            [{put, 1}];
        pareto ->
            [{get, N*3}, {update, 4}];
        uniform ->
             [{get, N*3}, {update, 1}]
    end.
