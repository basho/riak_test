-module(ts_A_create_table_already_created).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_create/3,
			  confirm_activate/3
			  ]).

%%
%% should error if you try and create a table twice
%%

confirm() ->
    ClusterType = single,
    DDL = get_ddl(docs),
    Expected1 = {ok, "GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"},
    pass = confirm_create(ClusterType, DDL, Expected1),
    Expected2 = {ok,"GeoCheckin has been activated\n\nWARNING: Nodes in this cluster can no longer be\ndowngraded to a version of Riak prior to 2.0\n"},
    pass = confirm_activate(ClusterType, DDL, Expected2),
    Expected3 = {ok,"Error creating bucket type GeoCheckin:\nalready_active\n"},
    confirm_create(ClusterType, DDL, Expected3).
