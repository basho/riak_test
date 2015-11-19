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
    ClusterType = single,
    DDL = ts_util:get_ddl(docs),
    Obj = [ts_util:get_invalid_obj()],
    Got = ts_util:ts_put(ts_util:cluster_and_connect(ClusterType), no_ddl, DDL, Obj),
    ?assertMatch({error, _}, Got),
    pass.


