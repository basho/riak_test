-module(ts_query).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("profiler/include/profiler.hrl").

confirm() ->
    perf_profile({prefix, "./profiler_results"}),

    DDL  = ts_api_util:get_ddl(api),
    UseNativeEncoding=true,
    Data = [[<<"family1">>, <<"seriesX">>, 100, 1, <<"test1">>, 1.0, true]],
    ts_api_util:setup_cluster_for_single_query(single, n_val_one, DDL, true, Data, UseNativeEncoding),

    Query = "SELECT * FROM GeoCheckin WHERE time >= 0 AND time < 1000 AND myfamily = 'family1' AND myseries = 'seriesX'",

    profiler:perf_profile({start, 0}),
    ts_api_util:runQuery(Query, 10),
    profiler:perf_profile({stop, 0}),

    pass.
