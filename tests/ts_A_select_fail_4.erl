
%%% Test the greater than operator on a varchar, this
%%% is not allowed.

-module(ts_A_select_fail_4).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    Qry = 
    	"select * from GeoCheckin "
    	"where time > 1 and time < 10 "
    	"and myfamily = 'family1' "
    	"and myseries ='seriesX' "
    	"and weather > 'bob'", % can't do greater than on a varchar!
    Expected = "some error message, fix me",
    timeseries_util:confirm_select(
    	single, normal, DDL, Data, Qry, Expected).
