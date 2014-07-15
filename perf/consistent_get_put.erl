-module(consistent_get_put).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    HostList = rt_config:get(rt_hostnames),
    Count = length(HostList),
    BinSize = rt_config:get(perf_bin_size),

    Config = rtperf:standard_config(Count),
    lager:info("Generated configuration is: ~p", [Config]),

    [Nodes] = rt:build_clusters([Count]),
    lager:info("Built cluster: ~p", [Nodes]),

    SetSize = rtperf:target_size(rt_config:get(perf_target_pct),
                                 BinSize,
                                 rt_config:get(perf_ram_size),
                                 Count),
    [Node | _] = Nodes,
    rt:create_and_activate_bucket_type(Node,
                                       <<"sc">>,
                                       [{consistent, true}]),

    TestConfig =
        rt_bench:config(
          max,
          rt_config:get(perf_duration),
          [{Host, 10017} || Host <- HostList],
          {int_to_bin_bigendian, {truncated_pareto_int, SetSize}},
          rt_bench:valgen(rt_config:get(perf_bin_type), BinSize),
          %% 4:1 get/put
          [{get, 3}, {update, 1}],
          {<<"sc">>, <<"testbucket">>},
          riakc_pb),

    ok = rtperf:maybe_prepop(Nodes, BinSize, SetSize),

    ok = rtperf:run_test(Nodes, TestConfig, []),
    pass.
