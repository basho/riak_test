-module(ts_A_create_table_fail_2).

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
    Expected = {ok,"this should fail"},
    confirm_create(ClusterType, DDL, Expected).
