-module(ts_A_put_fail_1).

%%
%% this test tries to write to a non-existant bucket
%%

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  get_valid_obj/0,
			  confirm_put/5
			  ]).

confirm() ->
    ClusterType = single,
    Expected = "some error message",
    DDL = null,
    Obj = [get_valid_obj()],
    confirm_put(ClusterType, no_ddl, DDL, Obj, Expected).
