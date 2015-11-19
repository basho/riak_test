-module(ts_B_put_pass_1).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterType = multiple,
    TestType = normal,
    DDL = ts_util:get_ddl(docs),
    Obj = [ts_util:get_valid_obj()],
    Expected = ok,
    Got = ts_util:ts_put(ts_util:cluster_and_connect(ClusterType), TestType, DDL, Obj),
    ?assertEqual(Expected, Got),
    pass.

