-module(ts_A_select_fail_2).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    Cluster = single,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    % FIXME we get a badmatch from riak_ql_parser
    Qry = 
    	"selectah * from GeoCheckin "
    	"Where time > 1 and time < 10",
    Expected = "some error message, fix me",
    timeseries_util:confirm_select(
    	Cluster, TestType, DDL, Data, Qry, Expected).
