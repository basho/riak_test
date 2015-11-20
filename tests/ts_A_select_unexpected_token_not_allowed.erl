-module(ts_A_select_unexpected_token_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    ClusterType = single,
    TestType = normal,
    DDL = ts_util:get_ddl(docs),
    Data = ts_util:get_valid_select_data(),
    Qry =
        "selectah * from GeoCheckin "
        "Where time > 1 and time < 10",
    {error, Got} = ts_util:ts_query(ts_util:cluster_and_connect(ClusterType), TestType, DDL, Data, Qry),
    ?assertNotEqual(0, string:str(binary_to_list(Got), "Unexpected token")),
    pass.
