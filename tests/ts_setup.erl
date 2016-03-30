-module(ts_setup).
-behavior(riak_test).
-export([confirm/0]).

confirm() ->
    DDL  = ts_api_util:get_ddl(api),
    ts_api_util:setup_cluster_timeseries(single, n_val_one, DDL, true),
    pass.

