-module(ts_A_select_fail_3).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  get_valid_select_data/0,
			  get_invalid_qry/1,
			  confirm_select/6
			  ]).

confirm() ->
    DDL = get_ddl(docs),
    Data = get_valid_select_data(),
    Qry = get_invalid_qry(key_not_covered),
    Expected = {error, <<"missing_param: Missing parameter myfamily in where clause.">>},
    confirm_select(single, normal, DDL, Data, Qry, Expected).
