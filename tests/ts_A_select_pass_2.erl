-module(ts_A_select_pass_2).

-behavior(riak_test).

-export([confirm/0]).

%% Test selects over fields which are not in the
%% primary key.

confirm() ->
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    % weather is not part of the primary key, it is
    % randomly generated data so this should return
    % zero results
    Qry =
        "SELECT * FROM GeoCheckin "
        "WHERE time > 1 AND time < 10 "
        "AND myfamily = 'family1' "
        "AND myseries = 'seriesX' "
        "AND weather = 'summer rain'",
    Expected = {[], []},
    timeseries_util:confirm_select(
        single, normal, DDL, Data, Qry, Expected).
