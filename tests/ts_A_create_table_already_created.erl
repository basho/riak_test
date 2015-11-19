-module(ts_A_create_table_already_created).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

%%
%% should error if you try and create a table twice
%%

confirm() ->
    ClusterType = single,
    DDL = ts_util:get_ddl(docs),
    ClusterConn = ts_util:cluster_and_connect(ClusterType),
    Expected1 = {ok, "GeoCheckin has been activated\n\nWARNING: Nodes in this cluster can no longer be\ndowngraded to a version of Riak prior to 2.0\n"},
    Got1 = ts_util:create_and_activate_bucket_type(ClusterConn, DDL),
    ?assertEqual(Expected1, Got1),
    Expected2 = {ok, "Error creating bucket type GeoCheckin:\nalready_active\n"},
    Got2 = ts_util:create_bucket_type(ClusterConn, DDL),
    ?assertEqual(Expected2, Got2),
    pass.
