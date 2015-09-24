-module(ts_A_create_table_fail_3).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_create/3
			  ]).

%%
%% should error if you try and create a table twice
%%

confirm() ->
    ClusterType = single,
    DDL = get_ddl(docs),
    Expected1 = {ok, "GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"},
    pass = confirm_create(ClusterType, DDL, Expected1),
    Expected2 = {ok,"some error message, yeah?"},
    confirm_create(ClusterType, DDL, Expected2).
