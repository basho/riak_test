-module(ts_A_put_all_datatypes).

-behavior(riak_test).

-export([
     confirm/0
    ]).

-include_lib("eunit/include/eunit.hrl").

-define(SPANNING_STEP, (1000)).

confirm() ->
    Cluster = single,
    TestType = normal,
    DDL = "CREATE TABLE GeoCheckin (" ++
        "myfamily    varchar     not null, " ++
        "myseries    varchar     not null, " ++
        "time        timestamp   not null, " ++
        "myint       sint64      not null, " ++
        "myfloat     double      not null, " ++
        "mybool      boolean     not null, " ++
        "mytimestamp timestamp   not null, " ++
        "myoptional  sint64, " ++
        "PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')), " ++
        "myfamily, myseries, time))",
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    N = 10,
    Data = make_data(N, Family, Series, []),
    %% Expected is wrong but we can't write data at the moment
    Got = timeseries_util:confirm_put(Cluster, TestType, DDL, Data),
    ?assertEqual(ok, Got),
    pass.

make_data(0, _, _, Acc) ->
    Acc;
make_data(N, F, S, Acc) when is_integer(N) andalso N > 0 ->
    NewAcc = [
          F,
          S,
          1 + N * ?SPANNING_STEP,
          N,
          N + 0.1,
          timeseries_util:get_bool(N),
          N + 100000,
          timeseries_util:get_optional(N, N)
         ],
    make_data(N - 1, F, S, [NewAcc | Acc]).

         
