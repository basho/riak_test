-module(ts_C_activate_table_fail_1).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_activate/3
			  ]).

confirm() ->
    ClusterType = one_down,
    DDL = get_ddl(docs),
    Expected = {ok,"GeoCheckin has been created but cannot be activated yet\n"},
    confirm_activate(ClusterType, DDL, Expected).
