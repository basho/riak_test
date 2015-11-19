-module(ts_A_put_bad_date).

%%
%%
%%

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterType = single,
    TestType = normal,
    DDL = ts_util:get_ddl(docs),
    Obj =
        [[ts_util:get_varchar(),
          ts_util:get_varchar(),
          <<"abc">>,
          ts_util:get_varchar(),
          ts_util:get_float()]],
    Expected = {error, {1003, <<"Invalid data">>}},
    Got = ts_util:ts_put(ts_util:cluster_and_connect(ClusterType), TestType, DDL, Obj),
    ?assertEqual(Expected, Got),
    pass.
