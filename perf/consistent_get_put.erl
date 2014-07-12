-module(consistent_get_put).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

%% note that this test does not currently work

confirm() ->
    HostList = rt_config:get(rt_hostnames),
    Count = length(HostList),
    BinSize = rt_config:get(perf_binsize),

    Config = rtperf:standard_config(Count),

    Nodes = rtperf:build_cluster(Config),

    SetSize = rtperf:target_size(rt_config:get(perf_target_pct),
                                 BinSize,
                                 rt_config:get(perf_ram_size),
                                 Count),
    [Node | _] = Nodes,
    rt:create_and_activate_bucket_type(Node, <<"sc">>, [{consistent, true}]),

    TestConfig =
        rt_bench:config(
          max,
          rt_config:get(perf_duration),
          HostList,
          {int_to_bin_bigendian, {uniform_int, SetSize}},
          rt_bench:valgen(rt_config:get(perf_bin_type), BinSize),
          %% 4:1 get/put
          [{get, 3}, {update, 1}],
      {<<"sc">>, <<"testbucket">>}
         ),

    ok = rtperf:maybe_prepop(HostList, BinSize, SetSize),

    ok = rtperf:run_test(HostList, TestConfig, []),
    pass.
