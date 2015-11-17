-module(ts_A_create_table_split_key).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    ClusterType = single,
    DDL = timeseries_util:get_ddl(splitkey_fail),
    Expected = {ok,"Error creating bucket type GeoCheckin:\nLocal key does not match primary key\n"},
	Got = timeseries_util:confirm_create(ClusterType, DDL),
    ?assertEqual(Expected, Got),
    pass.
