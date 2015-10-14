-module(ts_A_put_fail_1).

%%
%% this test tries to write to a non-existant bucket
%%

-behavior(riak_test).

-export([
     confirm/0
    ]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node | _]  = timeseries_util:build_cluster(single),
    Bucket = list_to_binary(timeseries_util:get_bucket()),
    Obj = [timeseries_util:get_valid_obj()],
    io:format("2 - writing to bucket ~p with:~n- ~p~n", [Bucket, Obj]),
    C = rt:pbc(Node),
    ?assertMatch(
        {error,_},
        riakc_ts:put(C, Bucket, Obj)
    ),
    pass.

