-module(ts_B_activate_table_pass_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    ClusterType = multiple,
    DDL = ts_util:get_ddl(docs),
    Expected = {ok, "GeoCheckin has been activated\n\nWARNING: Nodes in this cluster can no longer be\ndowngraded to a version of Riak prior to 2.0\n"},
    Got = ts_util:create_and_activate_bucket_type(ts_util:build_cluster(ClusterType), DDL),
	?assertEqual(Expected, Got),
	pass.
