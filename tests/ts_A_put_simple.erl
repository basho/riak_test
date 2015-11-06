-module(ts_A_put_simple).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Cluster = single,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Obj = [timeseries_util:get_valid_obj()],
    timeseries_util:confirm_put(Cluster, TestType, DDL, Obj, ok).
