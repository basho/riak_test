-module(ts_B_select_pass_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    Cluster = multiple,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    Qry = timeseries_util:get_valid_qry(),
    Expected = {
        timeseries_util:get_cols(docs),
        timeseries_util:exclusive_result_from_data(Data, 2, 9)},
    Got = timeseries_util:confirm_select(Cluster, TestType, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    pass.
