-module(ts_A_put_pass_2).

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
	"myint       integer     not null, " ++
	"myfloat     float       not null, " ++
	"mybool      boolean     not null, " ++
	"mytimestamp timestamp   not null, " ++
	"myany       any         not null, " ++
	"myoptional  integer, " ++
	"PRIMARY KEY ((myfamily, myseries, quantum(time, 15, 'm')), " ++
	"myfamily, myseries, time))",
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    N = 10,
    Data = make_data(N, Family, Series, []),
    %% Expected is wrong but we can't write data at the moment
    ?assertEqual(ok, timeseries_util:confirm_put(Cluster, TestType, DDL, Data)),
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
	      get_bool(N), 
	      N + 100000, 
	      get_any(N), 
	      get_optional(N, N)
	     ],
    make_data(N - 1, F, S, [NewAcc | Acc]).

get_bool(N) when N < 5 -> true;
get_bool(_)            -> false.

get_any(N) -> term_to_binary(lists:seq(1, N)).

get_optional(N, X) ->
    case N rem 2 of
	0 -> X;
	1 -> []
    end.
	     
