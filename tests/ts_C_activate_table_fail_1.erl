-module(ts_C_activate_table_fail_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    ClusterType = one_down,
    DDL = timeseries_util:get_ddl(docs),
    Expected = {ok,"GeoCheckin has been created but cannot be activated yet\n"},
	Got = timeseries_util:confirm_activate(ClusterType, DDL, Expected),
	?assertEqual(Expected, Got),
	pass.
