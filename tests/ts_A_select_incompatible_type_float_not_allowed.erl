-module(ts_A_select_incompatible_type_float_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    DDL = ts_util:get_ddl(docs),
    Data = ts_util:get_valid_select_data(),
    Qry =
        "SELECT * FROM GeoCheckin "
        "WHERE time > 1 AND time < 10 "
        "AND myfamily = 'family1' "
        "AND myseries = 1.0", % error, should be a varchar
    Expected =
        {error,{1001,
         <<"invalid_query: \n",
           "incompatible_type: field myseries with type varchar cannot be compared to type float in where clause.">>}},
    Got = ts_util:ts_query(ts_util:cluster_and_connect(single), normal, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    pass.
