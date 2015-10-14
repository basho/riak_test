-module(ts_A_select_fail_6).

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
    Qry = get_invalid_qry(type_error),
    Expected = {error, <<"invalid_query: \nincompatible_operator: field weather with type binary cannot use operator '>' in where clause.">>},
    confirm_select(single, normal, DDL, Data, Qry, Expected).
