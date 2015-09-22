-module(ts_put_fail_3).

%%
%% this test tries to write total gibberish data to a bucket
%%

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_put/5
			  ]).

confirm() ->
    ClusterType = single,
    DDL = get_ddl(docs),
    Obj = {[some], <<"total">>, g, i, b, {e, r, i, s, h}},
    Expected = "some error message",
    confirm_put(ClusterType, normal, DDL, Obj, Expected).
