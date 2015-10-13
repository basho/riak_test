-module(ts_A_put_pass_1).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Cluster = single,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Obj = [timeseries_util:get_valid_obj()],
    ?assertEqual(
        ok,
        timeseries_util:confirm_put(Cluster, TestType, DDL, Obj)
    ),
    io:format("~p ~p", [node(), erlang:get_cookie()]),
    pass.
