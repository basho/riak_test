-module(timeseries_activate_table_pass_1).
-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_activate/3
			  ]).

confirm() ->
    Cluster = single,
    DDL = get_ddl(docs),
    Expected = {ok,"GeoCheckin has been activated\n\nWARNING: Nodes in this cluster can no longer be\ndowngraded to a version of Riak prior to 2.0\n"},
    confirm_activate(Cluster, DDL, Expected).
