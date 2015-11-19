-module(ts_A_select_compare_two_fields_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

%%% Comparing fields should yield an error message.
%%% FIXME failing because of RTS-388

confirm() ->
    DDL = ts_util:get_ddl(docs),
    Data = ts_util:get_valid_select_data(),
    Qry =
        "SELECT * FROM GeoCheckin "
        "WHERE time > 1 and time < 10 "
        "AND myfamily = 'fa2mily1' "
        "AND myseries ='seriesX' "
        "AND weather = myseries",
    Expected = "Expect that fields cannot be compared",
    Got = ts_util:ts_query(ts_util:cluster_and_connect(single), normal, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    pass.
