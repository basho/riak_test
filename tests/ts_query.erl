-module(ts_query).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("profiler/include/profiler.hrl").

confirm() ->
  profiler:profile({prefix, "./client_profiler_results"}),

  DDL  = ts_api_util:get_ddl(api),
  UseNativeEncoding=true,
  ts_api_util:setup_cluster_put_mod_time(single, n_val_one, DDL, 1, true, UseNativeEncoding),
  Query = "SELECT * FROM GeoCheckin WHERE time >= 0 AND time <= 1 AND myfamily = 'family1' AND myseries = 'seriesX'",

  profiler:profile({start, 0}),
  ts_api_util:runQuery(Query, 1),
  profiler:profile({stop, 0}),

  pass.
