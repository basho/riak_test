-module(ts_A_select_fail_7_where_has_no_lower_bounds).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    Cluster = multiple,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Data = [],
    Qry = "select * from GeoCheckin "
          "where time < 10 "
          "and myfamily = 'family1' "
          "and myseries ='seriesX' ",
    Expected = 
      {error, <<"\"incomplete_where_clause: Where clause has no lower bound.\"">>},
    timeseries_util:confirm_select(
        Cluster, TestType, DDL, Data, Qry, Expected).

