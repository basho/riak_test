-module(ts_puttest_ts_10_query).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("profiler/include/profiler.hrl").

confirm() ->
    perf_profile({prefix, "./profiler_results"}),
    DDL  = ts_api_util:get_ddl(api),
    C = ts_api_util:setup_cluster_put(single, n_val_one, DDL, 2, true),
    ts_api_util:confirm_Pass(C, {myint,   int,     '>', 1}),
    pass.

