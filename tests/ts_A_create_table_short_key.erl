-module(ts_A_create_table_short_key).

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
    Expected = {ok,"Error creating bucket type GeoCheckin:\nPrimary key is too short\n"},
    confirm_create(ClusterType, DDL, Expected).
