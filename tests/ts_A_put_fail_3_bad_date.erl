-module(ts_A_put_fail_3_bad_date).

%%
%% 
%%

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterType = single,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Obj =
        [[timeseries_util:get_varchar(),
          timeseries_util:get_varchar(),
          <<"abc">>,
          timeseries_util:get_varchar(),
          timeseries_util:get_float()]],
    ?assertEqual(
        ok,
        timeseries_util:confirm_put(ClusterType, TestType, DDL, Obj)
    ),
    pass.
