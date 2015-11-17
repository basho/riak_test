
%%% Execute a query where the primary key is not covered
%%% in the where clause.

-module(ts_A_select_missing_field_in_pk_not_allowed).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    % query with missing myfamily field
    Query =
        "select * from GeoCheckin "
        "where time > 1 and time < 10",
    Expected =
        {error,{1001,<<"missing_param: Missing parameter myfamily in where clause.">>}},
    Got = timeseries_util:confirm_select(single, normal, DDL, Data, Query),
    ?assertEqual(Expected, Got),
    pass.
