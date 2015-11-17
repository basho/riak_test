
%%% Test the if a varchar matches an integer
%%% is not allowed.

-module(ts_A_select_incompatible_type_integer_not_allowed).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    Qry =
        "select * from GeoCheckin "
        "where time > 1 and time < 10 "
        "and myfamily = 'family1' "
        "and myseries = 10 ",
    Expected = {error,
        {1001,<<"invalid_query: \n",
          "incompatible_type: field myseries with type varchar cannot be compared to type integer in where clause.">>}},
    timeseries_util:confirm_select(
        single, normal, DDL, Data, Qry, Expected).
