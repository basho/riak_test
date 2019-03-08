-module('2iperf').
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    lager:info("entering get_put:confirm()"),
    HostList = rt_config:get(rt_hostnames),
    Count = length(HostList),
    BinSize = rt_config:get(perf_bin_size),

    Config = rtperf:standard_config(Count),

    ok = rtperf:build_cluster(Config),

    SetSize = rtperf:target_size(rt_config:get(perf_target_pct),
                                 BinSize,
                                 rt_config:get(perf_ram_size),
                                 Count),
    LoadConfig = [],
        %% rt_bench:config(
        %%   50,
        %%   rt_config:get(perf_duration),
        %%   HostList,
        %%   {int_to_bin_bigendian, {truncated_pareto_int, SetSize}},
        %%   rt_bench:valgen(rt_config:get(perf_bin_type), BinSize),
        %%   %% 4:1 get/put
        %%   [{get, 3}, {update, 1}]
        %%  ),

    TwoIConfig =
	rt_bench:config(
	  max,
	  rt_config:get(perf_duration),
	  HostList,
          {truncated_pareto_int, SetSize},
          rt_bench:valgen(rt_config:get(perf_bin_type), BinSize),
	  [{{query_pb, 100}, 5}, {{query_pb, 1000}, 1},
	   {{put_pb, 2}, 1}, {get_pb, 5}],
	  <<"testbucket">>, '2i'
	 ),

    ok = rtperf:maybe_prepop(HostList, BinSize, SetSize),

    ok = rtperf:run_test(HostList, TwoIConfig, LoadConfig),
    pass.
