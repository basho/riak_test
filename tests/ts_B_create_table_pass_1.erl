-module(ts_B_create_table_pass_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
     confirm/0
    ]).

confirm() ->
    ClusterType = multiple,
    DDL = timeseries_util:get_ddl(docs),
    Expected = {ok, "GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"},
    Got = timeseries_util:confirm_create(ClusterType, DDL),
    ?assertEqual(Expected, Got),
    pass.
