-module(ts_A_put_invalid_data).

%%
%% this test tries to write well structured data that doesn't
%% meet the criteria defined in the DDL into a bucket
%%

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterType = single,
    DDL = ts_util:get_ddl(docs),
    Obj = [ts_util:get_invalid_obj()],
    Expected = {error, {1003,<<"Invalid data">>}},
    Got = ts_util:ts_put(ts_util:cluster_and_connect(ClusterType), normal, DDL, Obj),
    ?assertEqual(Expected, Got),
    pass.

