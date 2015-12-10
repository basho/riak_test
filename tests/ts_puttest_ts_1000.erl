-module(ts_puttest_ts_1000).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("profiler/include/profiler.hrl").

confirm() ->
    perf_profile({prefix, "./profiler_results"}),
    DDL  = ts_api_util:get_ddl(api),
    ts_api_util:setup_cluster_put(single, n_val_one, DDL, 1000, true),
    pass.
