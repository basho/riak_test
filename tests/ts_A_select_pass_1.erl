-module(ts_A_select_pass_1).

-behavior(riak_test).

-export([
	 confirm/0
	]).


confirm() ->
    DDL  = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    Qry  = timeseries_util:get_valid_qry(),
    Expected = lists:map(fun list_to_tuple/1, Data),
    confirm_select(single, normal, DDL, Data, Qry, Expected).
