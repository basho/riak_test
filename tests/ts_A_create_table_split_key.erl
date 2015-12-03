-module(ts_A_create_table_split_key).

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
    DDL = get_ddl(splitkey_fail),
    Expected = {ok,"Error creating bucket type GeoCheckin:\nLocal key does not match primary key\n"},
    confirm_create(ClusterType, DDL, Expected).
