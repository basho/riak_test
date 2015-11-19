-module(ts_A_select_where_has_no_lower_bounds_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    ClusterType = multiple,
    TestType = normal,
    DDL = ts_util:get_ddl(docs),
    Data = [],
    Qry = "select * from GeoCheckin "
          "where time < 10 "
          "and myfamily = 'family1' "
          "and myseries ='seriesX' ",
    Expected = 
      {error, {1001, <<"incomplete_where_clause: Where clause has no lower bound.">>}},
    Got = ts_util:ts_query(ts_util:cluster_and_connect(ClusterType), TestType, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    pass.

