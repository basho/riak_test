-module(ts_B_put_pass_1).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Cluster = multiple,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Obj = [timeseries_util:get_valid_obj()],
    Expected = ok,
    Got = timeseries_util:confirm_put(Cluster, TestType, DDL, Obj),
    ?assertEqual(Expected, Got),
    pass.

