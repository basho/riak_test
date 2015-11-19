-module(ts_A_put_simple).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterType = single,
    TestType = normal,
    DDL = ts_util:get_ddl(docs),
    Obj = [ts_util:get_valid_obj()],
    Got = ts_util:ts_put(ts_util:cluster_and_connect(ClusterType), TestType, DDL, Obj),
    ?assertEqual(ok, Got),
    pass.
