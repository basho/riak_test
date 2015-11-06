-module(ts_A_put_non_existent_bucket).

%%
%% this test tries to write to a non-existent bucket
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
    Got = riakc_ts:put(C, Bucket, Obj),
    ?assertMatch({error, _}, Got),
    pass.

