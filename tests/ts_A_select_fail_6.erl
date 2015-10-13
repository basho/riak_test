-module(ts_A_select_fail_6).

-behavior(riak_test).

-export([confirm/0]).

-import(timeseries_util, [
			  get_ddl/1,
			  get_valid_select_data/0,
			  get_invalid_qry/1,
			  confirm_select/6
			  ]).

confirm() ->
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    Qry =
        "SELECT * FROM GeoCheckin "
        "WHERE time > 1 AND time < 10 "
        "AND myfamily = 'family1' "
        "AND myseries = 1 ", % error, should be a varchar 
    Expected =
        {error,
         <<"invalid_query: \n",
           "incompatible_type: field myseries with type binary cannot be compared to type int in where clause.">>},
    timeseries_util:confirm_select(
        single, normal, DDL, Data, Qry, Expected).
