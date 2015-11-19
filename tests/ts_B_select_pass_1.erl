-module(ts_B_select_pass_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    ClusterType = multiple,
    TestType = normal,
    DDL = ts_util:get_ddl(docs),
    Data = ts_util:get_valid_select_data(),
    Qry = ts_util:get_valid_qry(),
    Expected = {
        ts_util:get_cols(docs),
        ts_util:exclusive_result_from_data(Data, 2, 9)},
    Got = ts_util:ts_query(ts_util:cluster_and_connect(ClusterType), TestType, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    pass.
