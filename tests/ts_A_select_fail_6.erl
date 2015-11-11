-module(ts_A_select_fail_6).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    Qry =
        "SELECT * FROM GeoCheckin "
        "WHERE time > 1 AND time < 10 "
        "AND myfamily = 'family1' "
        "AND myseries = 1 ", % error, should be a varchar
    Expected =
        {error,{1001,
         <<"invalid_query: \n",
           "incompatible_type: field myseries with type varchar cannot be compared to type integer in where clause.">>}},
    timeseries_util:confirm_select(
        single, normal, DDL, Data, Qry, Expected).
