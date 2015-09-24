-module(ts_B_select_pass_1).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  get_valid_select_data/0,
			  get_valid_qry/0,
			  confirm_select/6
			  ]).

confirm() ->
    DDL = get_ddl(docs),
    Data = get_valid_select_data(),
    Qry = get_valid_qry(),
    Expected = ok,
    confirm_select(multiple, normal, DDL, Data, Qry, Expected).
