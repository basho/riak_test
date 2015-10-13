-module(ts_A_select_fail_5).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    Qry =
        "SELECT * FROM GeoCheckin "
        "WHERE time > 1 and time < 10 "
        "AND myfamily = 'fa2mily1' "
        "AND myseries ='seriesX' "
        "AND weather = 'lololol'",
    Expected = "some error message, fix me",
    timeseries_util:confirm_select(
        single, normal, DDL, Data, Qry, Expected).
