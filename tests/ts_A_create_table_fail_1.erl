-module(ts_A_create_table_fail_1).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_create/3
			  ]).

confirm() ->
    ClusterType = single,
    DDL = get_ddl(shortkey_fail),
    Expected = {ok,"GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"},
    confirm_create(ClusterType, DDL, Expected).
