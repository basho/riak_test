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
    DDL = timeseries_util:get_ddl(docs),
    Expected1 = {ok, "GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"},
    Got1 = timeseries_util:confirm_create(ClusterType, DDL),
    ?assertEqual(Expected1, Got1),
    Expected2 = {ok, "GeoCheckin has been activated\n\nWARNING: Nodes in this cluster can no longer be\ndowngraded to a version of Riak prior to 2.0\n"},
    Got2 = timeseries_util:confirm_activate(ClusterType, DDL),
    ?assertEqual(Expected2, Got2),
    Expected3 = {ok, "Error creating bucket type GeoCheckin:\nalready_active\n"},
    Got3 = timeseries_util:confirm_create(ClusterType, DDL),
    ?assertEqual(Expected3, Got3),
    pass.
